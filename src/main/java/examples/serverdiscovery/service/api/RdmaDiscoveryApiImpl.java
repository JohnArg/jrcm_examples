package examples.serverdiscovery.service.api;

import examples.serverdiscovery.common.RdmaDiscoveryApi;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * A registry service that will be used by RDMA-capable servers to discover each other.
 */
public class RdmaDiscoveryApiImpl implements RdmaDiscoveryApi {

    private final List<InetSocketAddress> registeredServers;

    public RdmaDiscoveryApiImpl(){
        registeredServers = new ArrayList<>();
    }

    @Override
    public synchronized List<InetSocketAddress> registerServer(InetSocketAddress serverAddress) {
        ArrayList<InetSocketAddress> existingMembers;
        existingMembers = new ArrayList<>(registeredServers);
        registeredServers.add(serverAddress);
        return existingMembers;
    }

    @Override
    public synchronized boolean unregisterServer(InetSocketAddress serverAddress) {
        return registeredServers.remove(serverAddress);
    }

    @Override
    public synchronized List<InetSocketAddress> getRegisteredServers() {
        return new ArrayList<>(registeredServers);
    }

    @Override
    public synchronized int getServerPortByIp(InetAddress ipAddress) {
        for(InetSocketAddress socketAddress : registeredServers){
            if(socketAddress.getAddress().equals(ipAddress)){
                return socketAddress.getPort();
            }
        }
        return -1;
    }

}
