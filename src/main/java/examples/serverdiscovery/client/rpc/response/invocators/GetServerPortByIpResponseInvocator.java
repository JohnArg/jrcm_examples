package examples.serverdiscovery.client.rpc.response.invocators;

import examples.serverdiscovery.client.rpc.response.PendingResponseManager;
import examples.serverdiscovery.common.DiscoveryRpcPacket;
import examples.serverdiscovery.common.DiscoveryRpcPacketHeaders;
import examples.serverdiscovery.common.serializers.IntSerializer;
import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.invocation.RpcOperationInvocator;
import jarg.jrcm.rpc.packets.AbstractRpcPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

public class GetServerPortByIpResponseInvocator implements RpcOperationInvocator {

    private final Logger logger = LoggerFactory.getLogger(GetRegisteredServersResponseInvocator.class.getSimpleName());

    private IntSerializer intSerializer;
    private PendingResponseManager responseManager;

    public GetServerPortByIpResponseInvocator(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
        intSerializer = new IntSerializer();
    }

    @Override
    public void invokeOperation(AbstractRpcPacket packet) {
        DiscoveryRpcPacket rpcPacket = (DiscoveryRpcPacket) packet;
        DiscoveryRpcPacketHeaders headers = rpcPacket.getPacketHeaders();
        long operationId = headers.getOperationId();
        WorkRequestProxy workRequestProxy = rpcPacket.getWorkRequestProxy();

        try{
            // deserialize response
            intSerializer.setWorkRequestProxy(workRequestProxy);
            intSerializer.readFromWorkRequestBuffer();
            workRequestProxy.releaseWorkRequest();
            int port = intSerializer.getValue();
            // complete future that was waiting for this response
            CompletableFuture<Integer> responseFuture =
                    responseManager.getServerPortByIpPendingResponses().get(operationId);
            responseFuture.complete(port);
        } catch (RpcDataSerializationException e) {
            logger.error("Cannot deserialize response");
            workRequestProxy.releaseWorkRequest();
        }
    }
}
