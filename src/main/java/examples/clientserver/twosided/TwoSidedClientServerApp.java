package examples.clientserver.twosided;

import examples.clientserver.twosided.client.TwoSidedClient;
import examples.clientserver.twosided.server.TwoSidedServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example application for client/server RDMA communications with SEND/RECV through jRCM.
 */
public class TwoSidedClientServerApp {
    private static final Logger logger = LoggerFactory.getLogger(TwoSidedClientServerApp.class.getSimpleName());

    public static void main(String[] args) {
        if(args.length < 3){
           logger.info("Please provide a role (s for server, c for client),"+
                    " an ip (not localhost) and a port.");
           logger.info("E.g. for the server run with args : s 10.0.2.4 3000");
           logger.info("E.g. for a client of the previous server run with args : c 10.0.2.4 3000");
           System.exit(1);
        }

        String role = args[0];
        String host = args[1];
        String port = args[2];

        // run as a server
        if(role.equals("s")){
            TwoSidedServer server = new TwoSidedServer(host, port);
            try {
                server.init();
                server.operate();
            } catch (Exception e) {
                logger.error("Error in operating server.", e);
            }
        } else if(role.equals("c")) { // run as a client
            TwoSidedClient client = new TwoSidedClient(host, port);
            try {
                client.init();
                client.operate();
            } catch (Exception e) {
                logger.error("Error in operating client.", e);
            }
        }
    }
}
