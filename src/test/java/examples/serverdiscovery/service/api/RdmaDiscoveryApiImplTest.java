package examples.serverdiscovery.service.api;

import examples.serverdiscovery.service.api.RdmaDiscoveryApiImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Testing RdmaDiscovery API")
public class RdmaDiscoveryApiImplTest {

    @Test
    @Tag("rdma_discovery")
    @DisplayName("Testing new server registration without any pre-registered servers")
    public void registerOnEmptyTest(){
        RdmaDiscoveryApiImpl service = new RdmaDiscoveryApiImpl();
        String ipToTest = "177.128.3.72";
        int portToTest = 3000;
        InetSocketAddress serverAddress = new InetSocketAddress(ipToTest, portToTest);

        List<InetSocketAddress> previousMembers = service.registerServer(serverAddress);
        assertNotNull(previousMembers);
        assertEquals(previousMembers.size() ,0);
        assertEquals(portToTest, service.getServerPortByIp(serverAddress.getAddress()));
    }

    @Test
    @Tag("rdma_discovery")
    @DisplayName("Testing new server registration with pre-registered servers")
    public void registerWithPreExistingTest(){
        RdmaDiscoveryApiImpl service = new RdmaDiscoveryApiImpl();
        List<InetSocketAddress> initialMembers = new ArrayList<>();

        initialMembers.add(new InetSocketAddress("177.128.3.73", 3053));
        initialMembers.add(new InetSocketAddress("177.128.3.74", 3054));
        initialMembers.add(new InetSocketAddress("177.128.3.75", 3055));

        List<InetSocketAddress> expectedMembers = new ArrayList<>();
        for(InetSocketAddress address : initialMembers){
            List<InetSocketAddress> previousMembers = service.registerServer(address);
            assertNotNull(previousMembers);
            assertTrue(expectedMembers.containsAll(previousMembers));
            assertEquals(address.getPort(), service.getServerPortByIp(address.getAddress()));
            expectedMembers.add(address);
        }
    }

    @Test
    @Tag("rdma_discovery")
    @DisplayName("Testing unregistering a server")
    public void unregisterServerTest(){
        RdmaDiscoveryApiImpl service = new RdmaDiscoveryApiImpl();
        String ipToTest = "177.128.3.72";
        int portToTest = 3000;
        InetSocketAddress serverAddress = new InetSocketAddress(ipToTest, portToTest);

        // test with empty List
        assertFalse(service.unregisterServer(new InetSocketAddress(ipToTest, portToTest)));
        // test after adding the server
        service.registerServer(serverAddress);
        assertTrue(service.unregisterServer(serverAddress));
    }

    @Test
    @Tag("rdma_discovery")
    @DisplayName("Testing getting all registered servers.")
    public void getRegisteredServersTest(){
        RdmaDiscoveryApiImpl service = new RdmaDiscoveryApiImpl();
        List<InetSocketAddress> initialMembers = new ArrayList<>();

        // assert empty List
        List<InetSocketAddress> actualMembers = service.getRegisteredServers();
        assertTrue(initialMembers.containsAll(actualMembers));

        initialMembers.add(new InetSocketAddress("177.128.3.73", 3053));
        initialMembers.add(new InetSocketAddress("177.128.3.74", 3054));
        initialMembers.add(new InetSocketAddress("177.128.3.75", 3055));

        for(InetSocketAddress address : initialMembers){
            service.registerServer(address);
        }

        // assert that everyone that was added is returned
        assertTrue(initialMembers.containsAll(actualMembers));
    }

    @Test
    @Tag("rdma_discovery")
    @DisplayName("Testing getting correct server ports when searching by IP")
    public void getServerPortByIpTest(){
        RdmaDiscoveryApiImpl service = new RdmaDiscoveryApiImpl();
        String ipToTest = "177.128.3.72";
        int portToTest = 3000;
        InetSocketAddress serverAddress = new InetSocketAddress(ipToTest, portToTest);

        // test with empty List
        assertEquals(-1, service.getServerPortByIp(serverAddress.getAddress()));
        // test after adding the server
        service.registerServer(serverAddress);
        assertEquals(portToTest, service.getServerPortByIp(serverAddress.getAddress()));
    }
}
