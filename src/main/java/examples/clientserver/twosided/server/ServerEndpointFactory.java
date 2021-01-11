package examples.clientserver.twosided.server;


import com.ibm.disni.RdmaActiveEndpointGroup;
import com.ibm.disni.RdmaEndpointFactory;
import com.ibm.disni.verbs.RdmaCmId;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.RdmaCommunicatorDependencies;
import jarg.rdmarpc.networking.dependencies.netbuffers.impl.TwoSidedBufferManager;
import jarg.rdmarpc.networking.dependencies.netrequests.impl.QueuedProxyProvider;
import jarg.rdmarpc.networking.dependencies.svc.impl.TwoSidedSVCManager;

import java.io.IOException;

/**
 * Factory of server side RDMA endpoints.
 */
public class ServerEndpointFactory implements RdmaEndpointFactory<ActiveRdmaCommunicator> {

    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private int maxBufferSize;
    private int maxWorkRequests;

    public ServerEndpointFactory(RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup,
                                 int maxBufferSize, int maxWorkRequests) {
        this.endpointGroup = endpointGroup;
        this.maxBufferSize = maxBufferSize;
        this.maxWorkRequests = maxWorkRequests;
    }


    @Override
    public ActiveRdmaCommunicator createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        //set endpoint dependencies
        RdmaCommunicatorDependencies dependencies = new RdmaCommunicatorDependencies();
        QueuedProxyProvider proxyProvider = new QueuedProxyProvider(maxWorkRequests);

        dependencies.setMaxBufferSize(maxBufferSize)
                .setMaxWorkRequests(maxWorkRequests)
                .setProxyProvider(proxyProvider)
                .setBufferManager(new TwoSidedBufferManager(maxBufferSize, maxWorkRequests))
                .setSvcManager(new TwoSidedSVCManager(maxBufferSize, maxWorkRequests))
                .setWorkCompletionHandler(new ServerCompletionHandler(proxyProvider));

        return new ActiveRdmaCommunicator(endpointGroup, id, serverSide, dependencies);
    }
}
