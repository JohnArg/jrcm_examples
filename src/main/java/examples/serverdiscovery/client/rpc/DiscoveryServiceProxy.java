package examples.serverdiscovery.client.rpc;

import examples.serverdiscovery.client.rpc.response.PendingResponseManager;
import examples.serverdiscovery.common.DiscoveryOperationType;
import examples.serverdiscovery.common.DiscoveryRpcPacket;
import examples.serverdiscovery.common.DiscoveryRpcPacketFactory;
import examples.serverdiscovery.common.RdmaDiscoveryApi;
import examples.serverdiscovery.common.serializers.InetAddressSerializer;
import examples.serverdiscovery.common.serializers.InetSocketAddressListSerializer;
import jarg.rdmarpc.networking.communicators.RdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.packets.RpcMessageType;
import jarg.rdmarpc.rpc.request.RequestIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Acts the client stub for discovery RPCs.
 */
public class DiscoveryServiceProxy implements RdmaDiscoveryApi {

    private Logger logger = LoggerFactory.getLogger(DiscoveryServiceProxy.class);

    private RdmaCommunicator rdmaCommunicator;
    private DiscoveryRpcPacketFactory packetFactory;
    private PendingResponseManager pendingResponseManager;

    public DiscoveryServiceProxy(RdmaCommunicator rdmaCommunicator, PendingResponseManager pendingResponseManager,
                                 RequestIdGenerator<Long> requestIdGenerator) {
        this.rdmaCommunicator = rdmaCommunicator;
        this.pendingResponseManager = pendingResponseManager;
        this.packetFactory = new DiscoveryRpcPacketFactory(requestIdGenerator);
    }

    @Override
    public List<InetSocketAddress> registerServer(InetSocketAddress serverAddress) {
        DiscoveryRpcPacket requestPacket = generateRequestPacket(DiscoveryOperationType.REGISTER_SERVER);
        // Send request parameters to serializer
        InetSocketAddressListSerializer parameterSerializer =
                new InetSocketAddressListSerializer(requestPacket.getWorkRequestProxy());
        parameterSerializer.setAddresses(Collections.singletonList(serverAddress));
        // Serialize the whole packet into the Work Request buffer
        try {
            requestPacket.writeToWorkRequestBuffer(parameterSerializer);
        } catch (RpcDataSerializationException e) {
            requestPacket.getWorkRequestProxy().releaseWorkRequest();
            logger.error("Cannot serialize RPC packet.", e);
            return null;
        }
        // Save a CompletableFuture for the RPC response - Do this BEFORE sending the request!
        long operationId = requestPacket.getPacketHeaders().getOperationId();
        CompletableFuture<List<InetSocketAddress>> pendingResponse = new CompletableFuture<>();
        pendingResponseManager.registerServerPendingResponses().put(operationId, pendingResponse);
        List<InetSocketAddress> previousAddresses = null;
        // Send the Work Request to the NIC
        rdmaCommunicator.postNetOperationToNIC(requestPacket.getWorkRequestProxy());
        // Wait for response
        try {
            previousAddresses = pendingResponse.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to get response.", e);
        }
        return previousAddresses;
    }

    @Override
    public boolean unregisterServer(InetSocketAddress serverAddress) {
        DiscoveryRpcPacket requestPacket = generateRequestPacket(DiscoveryOperationType.UNREGISTER_SERVER);
        // Send request parameters to serializer
        InetSocketAddressListSerializer parameterSerializer =
                new InetSocketAddressListSerializer(requestPacket.getWorkRequestProxy());
        parameterSerializer.setAddresses(Collections.singletonList(serverAddress));
        // Serialize the whole packet into the Work Request buffer
        try {
            requestPacket.writeToWorkRequestBuffer(parameterSerializer);
        } catch (RpcDataSerializationException e) {
            requestPacket.getWorkRequestProxy().releaseWorkRequest();
            logger.error("Cannot serialize RPC packet.", e);
            return false;
        }
        // Save a CompletableFuture for the RPC response - Do this BEFORE sending the request!
        long operationId = requestPacket.getPacketHeaders().getOperationId();
        CompletableFuture<Boolean> pendingResponse = new CompletableFuture<>();
        pendingResponseManager.unregisterServerPendingResponses().put(operationId, pendingResponse);
        boolean success = false;
        // Send the Work Request to the NIC
        rdmaCommunicator.postNetOperationToNIC(requestPacket.getWorkRequestProxy());
        // Wait for response
        try {
            success = pendingResponse.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to get response.", e);
        }
        return success;
    }

    @Override
    public List<InetSocketAddress> getRegisteredServers() {
        DiscoveryRpcPacket requestPacket = generateRequestPacket(DiscoveryOperationType.GET_SERVERS);
        // Serialize the whole packet into the Work Request buffer
        try {
            requestPacket.writeToWorkRequestBuffer(null);
        } catch (RpcDataSerializationException e) {
            requestPacket.getWorkRequestProxy().releaseWorkRequest();
            logger.error("Cannot serialize RPC packet.", e);
            return null;
        }
        // Save a CompletableFuture for the RPC response - Do this BEFORE sending the request!
        long operationId = requestPacket.getPacketHeaders().getOperationId();
        CompletableFuture<List<InetSocketAddress>> pendingResponse = new CompletableFuture<>();
        pendingResponseManager.getRegisteredServersPendingResponses().put(operationId, pendingResponse);
        List<InetSocketAddress> previousAddresses = null;
        // Send the Work Request to the NIC
        rdmaCommunicator.postNetOperationToNIC(requestPacket.getWorkRequestProxy());
        // Wait for response
        try {
            previousAddresses = pendingResponse.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to get response.", e);
        }
        return previousAddresses;
    }

    @Override
    public int getServerPortByIp(InetAddress ipAddress) {
        DiscoveryRpcPacket requestPacket = generateRequestPacket(DiscoveryOperationType.GET_SERVER_PORT);
        // Send request parameters to serializer
        InetAddressSerializer parameterSerializer =
                new InetAddressSerializer(requestPacket.getWorkRequestProxy());
        parameterSerializer.setAddress(ipAddress);
        // Serialize the whole packet into the Work Request buffer
        try {
            requestPacket.writeToWorkRequestBuffer(parameterSerializer);
        } catch (RpcDataSerializationException e) {
            requestPacket.getWorkRequestProxy().releaseWorkRequest();
            logger.error("Cannot serialize RPC packet.", e);
            return -1;
        }
        // Save a CompletableFuture for the RPC response - Do this BEFORE sending the request!
        long operationId = requestPacket.getPacketHeaders().getOperationId();
        CompletableFuture<Integer> pendingResponse = new CompletableFuture<>();
        pendingResponseManager.getServerPortByIpPendingResponses().put(operationId, pendingResponse);
        int port = -1;
        // Send the Work Request to the NIC
        rdmaCommunicator.postNetOperationToNIC(requestPacket.getWorkRequestProxy());
        // Wait for response
        try {
            port = pendingResponse.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to get response.", e);
        }
        return port;
    }

    /**
     * Helper function that generates a request RPC packet to send to the remote side.
     */
    private DiscoveryRpcPacket generateRequestPacket(int operationType){
        // Get an available Work Request from the communicator
        WorkRequestProxy workRequestProxy = rdmaCommunicator.getWorkRequestProxyProvider()
                .getPostSendRequestBlocking(WorkRequestType.TWO_SIDED_SEND_SIGNALED);
        // Generate a request packet
        return packetFactory.generatePacket(workRequestProxy, RpcMessageType.REQUEST, operationType);
    }
}
