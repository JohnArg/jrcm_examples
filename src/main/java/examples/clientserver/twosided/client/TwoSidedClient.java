package examples.clientserver.twosided.client;

import com.ibm.disni.RdmaActiveEndpointGroup;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * A client that uses two-sided RDMA operations to communicate with a server.
 */
public class TwoSidedClient {
    private static final Logger logger = LoggerFactory.getLogger(TwoSidedClient.class.getSimpleName());

    private String serverHost;
    private String serverPort;
    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private ClientEndpointFactory factory;
    ActiveRdmaCommunicator clientEndpoint;
    private int messagesToSend;

    public TwoSidedClient(String host, String port){
        this.serverHost = host;
        this.serverPort = port;
    }

    /**
     * Initializes client properties.
     * @throws IOException
     */
    public void init() throws IOException {
        // Settings
        int timeout = 1000;
        boolean polling = false;
        int maxWRs = 10;
        int cqSize = maxWRs;
        int maxSge = 1;
        int maxBufferSize = 200;
        messagesToSend = 5;
        // Create endpoint
        endpointGroup =  new RdmaActiveEndpointGroup<>(timeout, polling,
                maxWRs, maxSge, cqSize);
        factory = new ClientEndpointFactory(endpointGroup, maxBufferSize, maxWRs, messagesToSend);
        endpointGroup.init(factory);
        clientEndpoint = endpointGroup.createEndpoint();
    }

    /**
     * Runs the client operation.
     * @throws Exception
     */
    public void operate() throws Exception {

        // connect to server
        InetAddress serverIp = InetAddress.getByName(serverHost);
        InetSocketAddress serverSockAddr = new InetSocketAddress(serverIp,
                Integer.parseInt(serverPort));
        clientEndpoint.connect(serverSockAddr, 1000);
        logger.info("Client connected to server in address : "
                + clientEndpoint.getDstAddr().toString());

        // send messages ------------------------------------
        for(int i=0; i < messagesToSend; i++){
            // get free Work Request id for a 'send' operation
            WorkRequestProxy workRequestProxy = clientEndpoint.getWorkRequestProxyProvider()
                    .getPostSendRequestBlocking(TWO_SIDED_SEND_SIGNALED);
            // fill tha data buffer with data to send across
            ByteBuffer sendBuffer = workRequestProxy.getBuffer();
            sendBuffer.putInt(workRequestProxy.getId()+10);  // use this to identify the message
            String helloMessage = "Hello message ";
            for(int j=0; j < helloMessage.length(); j ++){
                sendBuffer.putChar(helloMessage.charAt(j));
            }
            sendBuffer.flip();
            // See what you send
            int irrelevant = sendBuffer.getInt();
            String text = sendBuffer.asCharBuffer().toString();
            logger.info("Will send : "+text);
            sendBuffer.rewind();
            // send the data across
            workRequestProxy.post();
        }

    }

    public void finalize(){
        //close endpoint/group
        try {
            clientEndpoint.close();
            endpointGroup.close();
        } catch (IOException | InterruptedException e) {
           logger.error("Could not close endpoint or endpoint group.", e);
        }
    }
}
