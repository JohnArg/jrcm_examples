package examples.serverdiscovery.client;

import examples.serverdiscovery.client.networking.ServiceConnectionComponent;
import examples.serverdiscovery.client.rpc.DiscoveryServiceProxy;
import examples.serverdiscovery.client.rpc.request.DiscoveryRequestIdGenerator;
import examples.serverdiscovery.client.rpc.response.PendingResponseManager;
import examples.serverdiscovery.common.DiscoveryCommunicatorDependencies;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.rpc.request.RequestIdGenerator;

import java.net.InetSocketAddress;

public class ClientComponent {

    private ServiceConnectionComponent serviceConnectionComponent;
    private PendingResponseManager responseManager;
    private RequestIdGenerator<Long> requestIdGenerator;
    private InetSocketAddress serviceAddress;
    // Rdma endpoint properties
    private int maxWorkRequests;
    private int cqSize;
    private int timeout;
    private boolean polling;
    private int maxSge;
    private int maxNetworkBufferSize;

    public ClientComponent(InetSocketAddress serviceAddress, int maxWorkRequests, int cqSize, int timeout,
                           boolean polling, int maxSge, int maxNetworkBufferSize) {
        this.serviceAddress = serviceAddress;
        this.maxWorkRequests = maxWorkRequests;
        this.cqSize = cqSize;
        this.timeout = timeout;
        this.polling = polling;
        this.maxSge = maxSge;
        this.maxNetworkBufferSize = maxNetworkBufferSize;
        serviceConnectionComponent = new ServiceConnectionComponent(serviceAddress, maxWorkRequests, cqSize,
                timeout, polling, maxSge, maxNetworkBufferSize);
    }

    public DiscoveryServiceProxy generateDiscoveryServiceProxy(){
        serviceConnectionComponent.connect();
        ActiveRdmaCommunicator communicator = serviceConnectionComponent.getRdmaCommunicator();
        DiscoveryCommunicatorDependencies dependencies = (DiscoveryCommunicatorDependencies)
                communicator.getDependencies();
        responseManager = dependencies.getResponseManager();
        requestIdGenerator = new DiscoveryRequestIdGenerator(0);
        return new DiscoveryServiceProxy(serviceConnectionComponent.getRdmaCommunicator(), responseManager,
                requestIdGenerator);
    }
}
