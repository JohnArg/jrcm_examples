package examples.serverdiscovery.client.rpc.response;

import examples.serverdiscovery.client.rpc.response.invocators.GetRegisteredServersResponseInvocator;
import examples.serverdiscovery.client.rpc.response.invocators.GetServerPortByIpResponseInvocator;
import examples.serverdiscovery.client.rpc.response.invocators.RegisterServerResponseInvocator;
import examples.serverdiscovery.client.rpc.response.invocators.UnregisterServerResponseInvocator;
import examples.serverdiscovery.common.DiscoveryOperationType;
import examples.serverdiscovery.common.DiscoveryRpcPacket;
import examples.serverdiscovery.common.DiscoveryRpcPacketHeaders;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.packets.PacketDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Used after deserializing the headers of a received ${@link DiscoveryRpcPacket}.
 * It reads those headers to identify the type of received response and call the appropriate
 * response handler.
 */
public class ResponsePacketDispatcher implements PacketDispatcher<DiscoveryRpcPacket> {
    private final Logger logger = LoggerFactory.getLogger(ResponsePacketDispatcher.class);

    private PendingResponseManager responseManager;
    // response invocators
    private RegisterServerResponseInvocator registerServerResponseInvocator;
    private UnregisterServerResponseInvocator unregisterServerResponseInvocator;
    private GetRegisteredServersResponseInvocator getRegisteredServersResponseInvocator;
    private GetServerPortByIpResponseInvocator getServerPortByIpResponseInvocator;

    public ResponsePacketDispatcher(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
        registerServerResponseInvocator = new RegisterServerResponseInvocator(responseManager);
        unregisterServerResponseInvocator = new UnregisterServerResponseInvocator(responseManager);
        getRegisteredServersResponseInvocator = new GetRegisteredServersResponseInvocator(responseManager);
        getServerPortByIpResponseInvocator = new GetServerPortByIpResponseInvocator(responseManager);
    }

    @Override
    public void dispatchPacket(DiscoveryRpcPacket packet) {
        // deserialize packet headers
        DiscoveryRpcPacketHeaders headers = packet.getPacketHeaders();
        try {
            headers.readFromWorkRequestBuffer();
        } catch (RpcDataSerializationException e) {
            logger.error("Could not deserialize packet headers.", e);
            return;
        }
        // dispatch according to headers
        switch(headers.getOperationType()){
            case DiscoveryOperationType.REGISTER_SERVER:
                registerServerResponseInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.UNREGISTER_SERVER:
                unregisterServerResponseInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.GET_SERVERS:
                getRegisteredServersResponseInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.GET_SERVER_PORT:
                getServerPortByIpResponseInvocator.invokeOperation(packet);
                break;
            default:
        }
    }

    public PendingResponseManager getResponseManager() {
        return responseManager;
    }
}
