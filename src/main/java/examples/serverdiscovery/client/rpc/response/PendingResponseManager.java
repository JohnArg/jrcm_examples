package examples.serverdiscovery.client.rpc.response;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Manages pending responses to RPC calls.
 */
public class PendingResponseManager {
    // Maps of <operation IDs, response futures>
    private Map<Long, CompletableFuture<List<InetSocketAddress>>> registerServerPendingResponses;
    private Map<Long, CompletableFuture<Boolean>> unregisterServerPendingResponses;
    private Map<Long, CompletableFuture<List<InetSocketAddress>>> getRegisteredServersPendingResponses;
    private Map<Long, CompletableFuture<Integer>> getServerPortByIpPendingResponses;

    public PendingResponseManager() {
        registerServerPendingResponses = new HashMap<>();
        unregisterServerPendingResponses = new HashMap<>();
        getRegisteredServersPendingResponses = new HashMap<>();
        getServerPortByIpPendingResponses = new HashMap<>();
    }

    public Map<Long, CompletableFuture<List<InetSocketAddress>>> registerServerPendingResponses() {
        return registerServerPendingResponses;
    }

    public Map<Long, CompletableFuture<Boolean>> unregisterServerPendingResponses() {
        return unregisterServerPendingResponses;
    }

    public Map<Long, CompletableFuture<List<InetSocketAddress>>> getRegisteredServersPendingResponses() {
        return getRegisteredServersPendingResponses;
    }

    public Map<Long, CompletableFuture<Integer>> getServerPortByIpPendingResponses() {
        return getServerPortByIpPendingResponses;
    }
}
