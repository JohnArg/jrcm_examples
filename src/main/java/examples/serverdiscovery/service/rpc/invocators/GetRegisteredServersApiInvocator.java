package examples.serverdiscovery.service.rpc.invocators;

import examples.serverdiscovery.common.DiscoveryRpcPacket;
import examples.serverdiscovery.common.RdmaDiscoveryApi;
import examples.serverdiscovery.common.serializers.InetSocketAddressListSerializer;
import examples.serverdiscovery.service.rpc.response.SinglePacketResponseTask;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.invocation.AbstractThreadPoolInvocator;
import jarg.jrcm.rpc.packets.AbstractRpcPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Reads information from an {@link DiscoveryRpcPacket}, deserializes parameters and invokes
 * {@link RdmaDiscoveryApi#getRegisteredServers()}. It then sends back a response to
 * the caller.
 */
public class GetRegisteredServersApiInvocator extends AbstractThreadPoolInvocator {

    private static final Logger logger = LoggerFactory.getLogger(GetRegisteredServersApiInvocator.class.getSimpleName());

    private RdmaDiscoveryApi serviceApi;

    public GetRegisteredServersApiInvocator(ExecutorService workersExecutor, RdmaDiscoveryApi serviceApi) {
        super(workersExecutor);
        this.serviceApi = serviceApi;
    }

    @Override
    public void invokeOperationTask(AbstractRpcPacket packet) {
        // Get the packet's work request data
        WorkRequestProxy workRequestProxy = packet.getWorkRequestProxy();
        // Free WR id, we have the objects we need
        workRequestProxy.releaseWorkRequest();
        // invoke the service's API
        List<InetSocketAddress> previousMembers = serviceApi.getRegisteredServers();
        // get a serializer for the response and set the response to it
        InetSocketAddressListSerializer responseSerializer = new InetSocketAddressListSerializer();
        responseSerializer.setAddresses(previousMembers);
        // send the response to the caller in another task
        SinglePacketResponseTask responseTask = new SinglePacketResponseTask((DiscoveryRpcPacket) packet,
                responseSerializer, TWO_SIDED_SEND_SIGNALED, false);
        getWorkersExecutor().submit(responseTask);
    }
}
