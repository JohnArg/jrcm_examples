package examples.clientserver.twosided.server;

import com.ibm.disni.verbs.IbvWC;
import jarg.rdmarpc.networking.dependencies.netrequests.AbstractWorkCompletionHandler;
import jarg.rdmarpc.networking.dependencies.netrequests.AbstractWorkRequestProxyProvider;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.ByteBuffer;
import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_RECV;
import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;


/**
 * Handles the completion of networking requests for a server RDMA endpoint.
 */
public class ServerCompletionHandler extends AbstractWorkCompletionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ServerCompletionHandler.class);

    @Override
    public void handleCqEvent(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy receiveProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // if this is a completion for a SEND
        if(receiveProxy.getWorkRequestType().equals(TWO_SIDED_SEND_SIGNALED)){
            logger.info("My message with WR id "+ receiveProxy.getId() +" was sent.");
            receiveProxy.releaseWorkRequest();
            // else if this is a completion for a RECV
        }else if(receiveProxy.getWorkRequestType().equals(TWO_SIDED_RECV)){
            ByteBuffer receiveBuffer = receiveProxy.getBuffer();
            // get the data out of the receive buffer
            int bufferId = receiveBuffer.getInt();
            receiveBuffer.limit(32);
            ByteBuffer textBuffer = receiveBuffer.slice();
            String text = textBuffer.asCharBuffer().toString();
            logger.info("Received completion for WR "+ receiveProxy.getId() +
                    ". Buffer id : " + bufferId +
                    ". The data is : " + text);
            // we don't need the received data anymore
            receiveProxy.releaseWorkRequest();
            // create and send a response for the received message ----------------------------
            String response = "ACK " + bufferId;
            WorkRequestProxy responseProxy = getProxyProvider().getPostSendRequestBlocking(TWO_SIDED_SEND_SIGNALED);
            // fill response buffer with data
            ByteBuffer sendBuffer = responseProxy.getBuffer();
            for(int j=0; j < response.length(); j ++){
                sendBuffer.putChar(response.charAt(j));
            }
            sendBuffer.flip();
            // send the response
            responseProxy.getEndpoint().postNetOperationToNIC(responseProxy);
        }
    }

    @Override
    public void handleCqEventError(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy receiveProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // Must free the request
        receiveProxy.releaseWorkRequest();
        logger.error("Error in network request completion");
    }

}
