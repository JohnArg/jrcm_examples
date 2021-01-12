package examples.serverdiscovery.service;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;
import examples.serverdiscovery.service.networking.ServiceCommunicatorFactory;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Server component that accepts RDMA connections from clients of the
 * Discovery Service.
 */
public class ServerComponent {
    private static final Logger logger = LoggerFactory.getLogger(ServerComponent.class);

    RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private List<ActiveRdmaCommunicator> inboundConnections;
    // server listening settings
    private InetSocketAddress listeningAddress;
    private int backlog;
    // Rdma endpoint properties
    RdmaServerEndpoint<ActiveRdmaCommunicator> serverEndpoint;
    private int maxWorkRequests;
    private int cqSize;
    private int timeout;
    private boolean polling;
    private int maxSge;
    private int maxNetworkBufferSize;
    // Multi-threaded request processing
    private ExecutorService requestProcessingWorkers;


    public ServerComponent(InetSocketAddress listeningAddress, int backlog,
                           int maxWorkRequests, int cqSize, int timeout, boolean polling,
                           int maxSge, int maxNetworkBufferSize, int processingThreadsNum) {
        this.listeningAddress = listeningAddress;
        this.backlog = backlog;
        this.maxWorkRequests = maxWorkRequests;
        this.cqSize = cqSize;
        this.timeout = timeout;
        this.polling = polling;
        this.maxSge = maxSge;
        this.maxNetworkBufferSize = maxNetworkBufferSize;
        this.requestProcessingWorkers = Executors.newFixedThreadPool(processingThreadsNum);
        inboundConnections = new ArrayList<>();
    }

    public void start(){
        // An endpoint group is needed to create RDMA endpoints
        try {
            endpointGroup = new RdmaActiveEndpointGroup<>(timeout, polling, maxWorkRequests, maxSge, cqSize);
        } catch (IOException e) {
            logger.error("Cannot create endpoint group.", e);
            return;
        }
        // The group requires an endpoint factory to create the endpoints
        ServiceCommunicatorFactory communicatorFactory = new ServiceCommunicatorFactory(requestProcessingWorkers,
                endpointGroup, maxNetworkBufferSize, maxWorkRequests);
        endpointGroup.init(communicatorFactory);
        // Get a server endpoint
        try {
            serverEndpoint = endpointGroup.createServerEndpoint();
        } catch (IOException e) {
            logger.error("Cannot create server endpoint.", e);
            return;
        }
        // bind the server endpoint to a provided ip and port
        try {
            serverEndpoint.bind(listeningAddress, backlog);
        } catch (Exception e) {
            logger.error("Cannot bind server to address : " + listeningAddress.toString());
            return;
        }
        // start accepting connections ------------------------------
        while(true){
            ActiveRdmaCommunicator clientEndpoint = null;
            try {
                clientEndpoint = serverEndpoint.accept();
            } catch (IOException e) {
                logger.warn("Server stopped accepting connections.", e);
                return;
            }
            inboundConnections.add(clientEndpoint);
        }
    }

    public void shutdown(){
        requestProcessingWorkers.shutdown();
        try {
            for (ActiveRdmaCommunicator clientEndpoint : inboundConnections) {
                clientEndpoint.close();
            }
            serverEndpoint.close();
            endpointGroup.close();
            logger.info("Shutting down server.");
        } catch (IOException | InterruptedException e) {
            logger.error("Error in closing server endpoint.", e);
        }
    }

    public void finalize(){
        shutdown();
    }

    public List<ActiveRdmaCommunicator> getInboundConnections() {
        return inboundConnections;
    }
}
