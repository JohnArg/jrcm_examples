package examples.serverdiscovery.service.networking;

import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import examples.serverdiscovery.common.DiscoveryCommunicatorDependencies;
import examples.serverdiscovery.common.networking.TwoSidedCompletionHandler;
import examples.serverdiscovery.service.api.RdmaDiscoveryApiImpl;
import examples.serverdiscovery.service.rpc.DiscoveryServicePacketDispatcher;
import jarg.jrcm.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.jrcm.networking.dependencies.netbuffers.impl.TwoSidedBufferManager;
import jarg.jrcm.networking.dependencies.netrequests.impl.QueuedProxyProvider;
import jarg.jrcm.networking.dependencies.svc.impl.TwoSidedSVCManager;

import java.io.IOException;
import java.util.concurrent.ExecutorService;


public class ServiceCommunicatorFactory implements RdmaEndpointFactory<ActiveRdmaCommunicator> {

    private RdmaDiscoveryApiImpl discoveryApi;
    // dependencies -------------
    private ExecutorService requestProcessingWorkers;
    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private int maxBufferSize;
    private int maxWorkRequests;

    public ServiceCommunicatorFactory(ExecutorService requestProcessingWorkers,
                                      RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup,
                                      int maxBufferSize, int maxWorkRequests) {
        this.discoveryApi = new RdmaDiscoveryApiImpl();
        this.requestProcessingWorkers = requestProcessingWorkers;
        this.endpointGroup = endpointGroup;
        this.maxBufferSize = maxBufferSize;
        this.maxWorkRequests = maxWorkRequests;
    }

    @Override
    public ActiveRdmaCommunicator createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        // Create a packet dispatcher for this endpoint
        DiscoveryServicePacketDispatcher packetDispatcher = new DiscoveryServicePacketDispatcher(discoveryApi,
                requestProcessingWorkers);
        // Create endpoint dependencies
        DiscoveryCommunicatorDependencies dependencies = new DiscoveryCommunicatorDependencies();
        dependencies.setPacketDispatcher(packetDispatcher);
        dependencies.setMaxBufferSize(maxBufferSize)
                .setMaxWorkRequests(maxWorkRequests)
                .setProxyProvider(new QueuedProxyProvider(maxWorkRequests))
                .setBufferManager(new TwoSidedBufferManager(maxBufferSize, maxWorkRequests))
                .setSvcManager(new TwoSidedSVCManager(maxBufferSize, maxWorkRequests))
                .setWorkCompletionHandler(new TwoSidedCompletionHandler(dependencies.getProxyProvider(),
                        packetDispatcher));
        // create endpoint
        return new ActiveRdmaCommunicator(endpointGroup, id, serverSide, dependencies);
    }
}
