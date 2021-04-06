package examples.serverdiscovery.service.rpc.invocators;

import examples.serverdiscovery.common.DiscoveryRpcPacket;
import examples.serverdiscovery.common.RdmaDiscoveryApi;
import examples.serverdiscovery.common.serializers.InetAddressSerializer;
import examples.serverdiscovery.common.serializers.InetSocketAddressListSerializer;
import examples.serverdiscovery.common.serializers.IntSerializer;
import examples.serverdiscovery.service.rpc.response.SinglePacketResponseTask;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.invocation.AbstractThreadPoolInvocator;
import jarg.jrcm.rpc.packets.AbstractRpcPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static jarg.jrcm.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Reads information from an {@link DiscoveryRpcPacket}, deserializes parameters and invokes
 * {@link RdmaDiscoveryApi#getServerPortByIp(InetAddress)}. It then sends back a response to
 * the caller.
 */
public class GetServerPortByIpApiInvocator extends AbstractThreadPoolInvocator {

    private static final Logger logger = LoggerFactory.getLogger(GetServerPortByIpApiInvocator.class.getSimpleName());

    private RdmaDiscoveryApi serviceApi;

    public GetServerPortByIpApiInvocator(ExecutorService workersExecutor, RdmaDiscoveryApi serviceApi) {
        super(workersExecutor);
        this.serviceApi = serviceApi;
    }

    @Override
    public void invokeOperationTask(AbstractRpcPacket packet) {
        // Pass the packet's work request data to the serializer
        WorkRequestProxy workRequestData = packet.getWorkRequestProxy();
        InetAddressSerializer serializer = new InetAddressSerializer();
        serializer.setWorkRequestProxy(workRequestData);

        try {
            // deserialize request parameters from the received packet
            serializer.readFromWorkRequestBuffer();
            InetAddress address = serializer.getAddress();
            // Free WR id, we have the objects we need
            workRequestData.releaseWorkRequest();
            // invoke the service's API
            int port = serviceApi.getServerPortByIp(address);
            // get a serializer for the response and set the response to it
            IntSerializer responseSerializer = new IntSerializer();
            responseSerializer.setValue(port);
            // send the response to the caller in another task
            SinglePacketResponseTask responseTask = new SinglePacketResponseTask((DiscoveryRpcPacket) packet,
                    responseSerializer, TWO_SIDED_SEND_SIGNALED, false);
            getWorkersExecutor().submit(responseTask);
        } catch (RpcDataSerializationException e) {
            // Free WR id
            workRequestData.releaseWorkRequest();
            // send the response to the caller in another task
            SinglePacketResponseTask responseTask = new SinglePacketResponseTask((DiscoveryRpcPacket) packet,
                    null, TWO_SIDED_SEND_SIGNALED,true);
            getWorkersExecutor().submit(responseTask);
            logger.error("Unable to invoke service API", e);
        }
    }
}
