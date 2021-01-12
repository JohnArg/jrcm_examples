package examples.serverdiscovery;

import examples.serverdiscovery.client.ClientComponent;
import examples.serverdiscovery.client.rpc.DiscoveryServiceProxy;
import examples.serverdiscovery.service.ServerComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;

public class ServerDiscoveryRpcApp {
    private static final Logger logger = LoggerFactory.getLogger(ServerDiscoveryRpcApp.class.getSimpleName());

    public static void main(String[] args) {
        // read cmd arguments -----------------------------------------
        if (args.length < 3) {
            logger.info("Please provide a role (s for service, c for client)," +
                    " an ip (not localhost) and a port.");
            logger.info("E.g. for the service run with args : s 10.0.2.4 3000");
            logger.info("E.g. for a client of the previous service run with args : c 10.0.2.4 3000");
            System.exit(1);
        }
        String role = args[0];
        String host = args[1];
        String port = args[2];
        // defaults --------------------------------------------------
        int backlog = 20;
        int maxWorkRequests = 20;
        int cqSize = maxWorkRequests;
        int timeout = 1000;
        boolean polling = false;
        int maxSge = 1;
        int maxNetworkBufferSize = 1024;
        int processingThreads = 4;
        // listening address ------------------------------------------
        InetAddress ipAddr;
        try {
            ipAddr = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            logger.error("Cannot find provided host ip.", e);
            return;
        }
        InetSocketAddress listeningAddress = new InetSocketAddress(ipAddr, Integer.parseInt(port));
        // run as a server
        if (role.equals("s")) {
            ServerComponent serverComponent = new ServerComponent(listeningAddress, backlog,
                    maxWorkRequests, cqSize, timeout, polling, maxSge, maxNetworkBufferSize,
                    processingThreads);
            logger.info("Starting server component.");
            serverComponent.start();
        } else if (role.equals("c")) { // run as a client
            ClientComponent component = new ClientComponent(listeningAddress,
                    maxWorkRequests, cqSize, timeout, polling, maxSge, maxNetworkBufferSize);
            logger.info("Starting client component.");
            // make RPC calls to the service
            invokeRPCs(component);
        }
    }

    /**
     * Helper method that invokes RPCs.
     */
    private static void invokeRPCs(ClientComponent component){
        DiscoveryServiceProxy proxy = component.generateDiscoveryServiceProxy();
        InetSocketAddress clientAddress = component.getClientAddress();

        System.out.println("This client will be using this address : " + clientAddress.toString());

        StringBuilder builder = new StringBuilder();
        builder.append("Which RPC do you want to call? Type one of the following numbers : \n");
        builder.append("1) register this server\n");
        builder.append("2) unregister this server\n");
        builder.append("3) get registered servers\n");
        builder.append("4) get a registered server's port\n");
        builder.append("5) exit\n");

        Scanner input = new Scanner(System.in);
        boolean terminate = false;

        while(!terminate){
            System.out.println(builder);
            try{
                int choice = input.nextInt();

                switch (choice){
                    case 1:
                        List<InetSocketAddress> previousMembers = proxy.registerServer(clientAddress);
                        System.out.println("Previous members : ");
                        previousMembers.forEach(System.out::println);
                        break;
                    case 2:
                        boolean success = proxy.unregisterServer(clientAddress);
                        System.out.println("Success : " + success);
                        break;
                    case 3:
                        List<InetSocketAddress> registeredMembers = proxy.getRegisteredServers();
                        System.out.println("Registered members : ");
                        registeredMembers.forEach(System.out::println);
                        break;
                    case 4:
                        System.out.println("Give the IP of the server for which to get the port :");
                        input.nextLine();
                        String ipStr = input.nextLine();
                        int port = proxy.getServerPortByIp(InetAddress.getByName(ipStr));
                        System.out.println("Registered server's port : " + port);
                        break;
                    case 5:
                        terminate = true;
                        break;
                    default:
                        throw new IOException("invalid number");
                }

            }catch (Exception e){
                System.err.println("Please provide a valid input.");
            }
        }
        component.shutDown();
    }
}
