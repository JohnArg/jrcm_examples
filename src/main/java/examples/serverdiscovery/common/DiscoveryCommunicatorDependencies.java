package examples.serverdiscovery.common;

import examples.serverdiscovery.client.rpc.response.PendingResponseManager;
import jarg.rdmarpc.networking.dependencies.RdmaCommunicatorDependencies;
import jarg.rdmarpc.rpc.packets.PacketDispatcher;

public class DiscoveryCommunicatorDependencies extends RdmaCommunicatorDependencies {

    private PendingResponseManager responseManager;
    private PacketDispatcher<DiscoveryRpcPacket> packetDispatcher;

    public PendingResponseManager getResponseManager() {
        return responseManager;
    }

    public DiscoveryCommunicatorDependencies setResponseManager(PendingResponseManager responseManager) {
        this.responseManager = responseManager;
        return this;
    }

    public PacketDispatcher<DiscoveryRpcPacket> getPacketDispatcher() {
        return packetDispatcher;
    }

    public DiscoveryCommunicatorDependencies setPacketDispatcher(PacketDispatcher<DiscoveryRpcPacket>
                                                                         packetDispatcher) {
        this.packetDispatcher = packetDispatcher;
        return this;
    }
}
