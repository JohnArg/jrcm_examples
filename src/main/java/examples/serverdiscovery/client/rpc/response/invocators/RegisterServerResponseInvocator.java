package examples.serverdiscovery.client.rpc.response.invocators;

import examples.serverdiscovery.client.rpc.response.PendingResponseManager;
import examples.serverdiscovery.common.DiscoveryRpcPacket;
import examples.serverdiscovery.common.DiscoveryRpcPacketHeaders;
import examples.serverdiscovery.common.serializers.InetSocketAddressListSerializer;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.invocation.RpcOperationInvocator;
import jarg.jrcm.rpc.packets.AbstractRpcPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;


public class RegisterServerResponseInvocator implements RpcOperationInvocator {
    private final Logger logger = LoggerFactory.getLogger(RegisterServerResponseInvocator.class.getSimpleName());

    private InetSocketAddressListSerializer inetSocketAddressListSerializer;
    private PendingResponseManager responseManager;

    public RegisterServerResponseInvocator(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
        this.inetSocketAddressListSerializer = new InetSocketAddressListSerializer();
    }

    @Override
    public void invokeOperation(AbstractRpcPacket packet) {
        DiscoveryRpcPacket rpcPacket = (DiscoveryRpcPacket) packet;
        DiscoveryRpcPacketHeaders headers = rpcPacket.getPacketHeaders();
        long operationId = headers.getOperationId();
        WorkRequestProxy workRequestProxy = rpcPacket.getWorkRequestProxy();

        try {
            // deserialize response
            inetSocketAddressListSerializer.setWorkRequestProxy(workRequestProxy);
            inetSocketAddressListSerializer.readFromWorkRequestBuffer();
            workRequestProxy.releaseWorkRequest();
            List<InetSocketAddress> previousServers = inetSocketAddressListSerializer.getAddresses();
            // complete future that was waiting for this response
            CompletableFuture<List<InetSocketAddress>> responseFuture =
                    responseManager.registerServerPendingResponses().get(operationId);
            responseFuture.complete(previousServers);
        } catch (RpcDataSerializationException e) {
            logger.error("Cannot deserialize response");
            workRequestProxy.releaseWorkRequest();
        }
    }
}
