package examples.serverdiscovery.client.rpc.response.invocators;

import examples.serverdiscovery.client.rpc.response.PendingResponseManager;
import examples.serverdiscovery.common.DiscoveryRpcPacket;
import examples.serverdiscovery.common.DiscoveryRpcPacketHeaders;
import examples.serverdiscovery.common.serializers.BooleanSerializer;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.invocation.RpcOperationInvocator;
import jarg.rdmarpc.rpc.packets.AbstractRpcPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class UnregisterServerResponseInvocator implements RpcOperationInvocator {
    private final Logger logger = LoggerFactory.getLogger(UnregisterServerResponseInvocator.class.getSimpleName());

    private BooleanSerializer booleanSerializer;
    private PendingResponseManager responseManager;

    public UnregisterServerResponseInvocator(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
        booleanSerializer = new BooleanSerializer();
    }

    @Override
    public void invokeOperation(AbstractRpcPacket packet) {
        DiscoveryRpcPacket rpcPacket = (DiscoveryRpcPacket) packet;
        DiscoveryRpcPacketHeaders headers = rpcPacket.getPacketHeaders();
        long operationId = headers.getOperationId();
        WorkRequestProxy workRequestProxy = rpcPacket.getWorkRequestProxy();

        try{
            // deserialize response
            booleanSerializer.setWorkRequestProxy(workRequestProxy);
            booleanSerializer.readFromWorkRequestBuffer();
            workRequestProxy.releaseWorkRequest();
            Boolean success = booleanSerializer.getFlag();
            // complete future that was waiting for this response
            CompletableFuture<Boolean> responseFuture =
                    responseManager.unregisterServerPendingResponses().get(operationId);
            responseFuture.complete(success);
        } catch (RpcDataSerializationException e) {
            logger.error("Cannot deserialize response");
            workRequestProxy.releaseWorkRequest();
        }
    }
}
