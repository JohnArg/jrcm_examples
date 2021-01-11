package examples.serverdiscovery.service.rpc;

import examples.serverdiscovery.common.DiscoveryOperationType;
import examples.serverdiscovery.common.DiscoveryRpcPacket;
import examples.serverdiscovery.common.DiscoveryRpcPacketHeaders;
import examples.serverdiscovery.common.RdmaDiscoveryApi;
import examples.serverdiscovery.service.rpc.invocators.GetRegisteredServersApiInvocator;
import examples.serverdiscovery.service.rpc.invocators.GetServerPortByIpApiInvocator;
import examples.serverdiscovery.service.rpc.invocators.RegisterServerApiInvocator;
import examples.serverdiscovery.service.rpc.invocators.UnregisterServerApiInvocator;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.invocation.RpcOperationInvocator;
import jarg.rdmarpc.rpc.packets.PacketDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * Used after deserializing the headers of a received ${@link DiscoveryRpcPacket}.
 * It reads those headers to identify the type of received request and call the appropriate
 * ${@link RpcOperationInvocator}.
 * Then, the invocator will call the service's API that corresponds to packet's operation type.
 */
public class DiscoveryServicePacketDispatcher implements PacketDispatcher<DiscoveryRpcPacket> {

    private final Logger logger = LoggerFactory.getLogger(DiscoveryServicePacketDispatcher.class);

    private RpcOperationInvocator registerServerApiInvocator;
    private RpcOperationInvocator unregisterServerApiInvocator;
    private RpcOperationInvocator getRegisteredServersApiInvocator;
    private RpcOperationInvocator getServerPortByIpApiInvocator;

    public DiscoveryServicePacketDispatcher(RdmaDiscoveryApi rdmaDiscoveryApi, ExecutorService workersExecutor) {
        this.registerServerApiInvocator = new RegisterServerApiInvocator(workersExecutor, rdmaDiscoveryApi);
        this.unregisterServerApiInvocator = new UnregisterServerApiInvocator(workersExecutor, rdmaDiscoveryApi);
        this.getRegisteredServersApiInvocator = new GetRegisteredServersApiInvocator(workersExecutor, rdmaDiscoveryApi);
        this.getServerPortByIpApiInvocator = new GetServerPortByIpApiInvocator(workersExecutor, rdmaDiscoveryApi);
    }

    @Override
    public void dispatchPacket(DiscoveryRpcPacket packet) {
        // read headers from packet data
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
                registerServerApiInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.UNREGISTER_SERVER:
                unregisterServerApiInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.GET_SERVERS:
                getRegisteredServersApiInvocator.invokeOperation(packet);
                break;
            case DiscoveryOperationType.GET_SERVER_PORT:
                getServerPortByIpApiInvocator.invokeOperation(packet);
                break;
            default:
        }
    }
}
