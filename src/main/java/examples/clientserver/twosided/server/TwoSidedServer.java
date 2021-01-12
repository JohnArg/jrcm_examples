package examples.clientserver.twosided.server;

import com.ibm.disni.RdmaActiveEndpointGroup;

import com.ibm.disni.RdmaServerEndpoint;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * A server that uses two-sided RDMA communications to talk with clients.
 */
public class TwoSidedServer {
    private static final Logger logger = LoggerFactory.getLogger(TwoSidedServer.class.getSimpleName());

    private String serverHost;
    private String serverPort;
    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private ServerEndpointFactory factory;
    private RdmaServerEndpoint<ActiveRdmaCommunicator> serverEndpoint;
    private List<ActiveRdmaCommunicator> clients;

    public TwoSidedServer(String host, String port){
        this.serverHost = host;
        this.serverPort = port;
    }

    /**
     * Initializes server properties.
     * @throws Exception
     */
    public void init() throws Exception {
        // Settings
        int timeout = 1000;
        boolean polling = false;
        int maxWRs = 10;
        int cqSize = maxWRs;
        int maxSge = 1;
        int maxBufferSize = 200;

        clients = new ArrayList<>();
        // Create endpoint
        endpointGroup = new RdmaActiveEndpointGroup<>(timeout, polling,
                maxWRs, maxSge, cqSize);
        factory = new ServerEndpointFactory(endpointGroup, maxBufferSize, maxWRs);
        endpointGroup.init(factory);
        serverEndpoint = endpointGroup.createServerEndpoint();

        // bind server to address/port
        InetAddress serverIp = InetAddress.getByName(serverHost);
        InetSocketAddress serverSockAddr = new InetSocketAddress(serverIp,
                Integer.parseInt(serverPort));
        serverEndpoint.bind(serverSockAddr, 10);
        logger.info("Server bound to address : "
                + serverSockAddr.toString());
    }

    /**
     * Runs the server operation.
     * @throws Exception
     */
    public void operate() throws Exception {

        while(true){
            // accept client connection
            ActiveRdmaCommunicator clientEndpoint = serverEndpoint.accept();
            clients.add(clientEndpoint);
            logger.info("Client connection accepted. Client : "
                    + clientEndpoint.getDstAddr().toString());
        }


    }

    public void finalize(){
        //Cleanup -------------------------------------------
        try {
            for (ActiveRdmaCommunicator clientEndpoint : clients) {
                clientEndpoint.close();
            }
            serverEndpoint.close();
            endpointGroup.close();
            logger.info("Server is shut down");
        }catch (Exception e){
            logger.error("Error in shutting down server.", e);
        }
    }
}
