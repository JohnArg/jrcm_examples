package examples.clientserver.twosided.client;




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
 * Factory of client side RDMA endpoints.
 */
public class ClientEndpointFactory implements RdmaEndpointFactory<ActiveRdmaCommunicator> {

    private RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup;
    private int maxBufferSize;
    private int maxWorkRequests;
    private int maxAcks;

    public ClientEndpointFactory(RdmaActiveEndpointGroup<ActiveRdmaCommunicator> endpointGroup,
                                 int maxBufferSize, int maxWorkRequests, int maxAcks) {
        this.endpointGroup = endpointGroup;
        this.maxBufferSize = maxBufferSize;
        this.maxWorkRequests = maxWorkRequests;
        this.maxAcks = maxAcks;
    }


    @Override
    public ActiveRdmaCommunicator createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
        //set endpoint dependencies
        RdmaCommunicatorDependencies dependencies = new RdmaCommunicatorDependencies();

        dependencies.setMaxBufferSize(maxBufferSize)
                .setMaxWorkRequests(maxWorkRequests)
                .setProxyProvider(new QueuedProxyProvider(maxWorkRequests))
                .setBufferManager(new TwoSidedBufferManager(maxBufferSize, maxWorkRequests))
                .setSvcManager(new TwoSidedSVCManager(maxBufferSize, maxWorkRequests))
                .setWorkCompletionHandler(new ClientCompletionHandler(maxAcks));

        return new ActiveRdmaCommunicator(endpointGroup, id, serverSide, dependencies);
    }
}
