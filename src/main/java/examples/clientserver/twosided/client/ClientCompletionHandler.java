package examples.clientserver.twosided.client;

import com.ibm.disni.verbs.IbvWC;
import jarg.rdmarpc.networking.communicators.impl.ActiveRdmaCommunicator;
import jarg.rdmarpc.networking.dependencies.netrequests.AbstractWorkCompletionHandler;
import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_RECV;
import static jarg.rdmarpc.networking.dependencies.netrequests.types.WorkRequestType.TWO_SIDED_SEND_SIGNALED;

/**
 * Handles the completion of networking requests for the client RDMA endpoints.
 */
public class ClientCompletionHandler extends AbstractWorkCompletionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientCompletionHandler.class.getSimpleName());

    private int ACK_COUNT;      // how many ACKs we got
    private int MAX_ACKS;       // how many ACKs to expect

    public ClientCompletionHandler(int maxACKs){
        ACK_COUNT = 0;
        MAX_ACKS = maxACKs;
    }

    @Override
    public void handleCqEvent(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy workRequestProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // if this is a completion for a SEND
        if(workRequestProxy.getWorkRequestType().equals(TWO_SIDED_SEND_SIGNALED)){
            logger.info("My message with WR id "+ workRequestProxy.getId() +" was sent.");
            workRequestProxy.releaseWorkRequest();
        // else if this is a completion for a RECV
        }else if(workRequestProxy.getWorkRequestType().equals(TWO_SIDED_RECV)){
            String text = workRequestProxy.getBuffer().asCharBuffer().toString();
            logger.info("Received completion for WR "+ workRequestProxy.getId() +". The data is : " + text);
            // Always free the Work Request id after we're done
            workRequestProxy.releaseWorkRequest();
            //For simplicity we don't check if the received message is the one expected
            ACK_COUNT++;
            if (ACK_COUNT == MAX_ACKS){
                System.exit(0);
            }
        }
    }

    @Override
    public void handleCqEventError(IbvWC workCompletionEvent) {
        // associate event with a Work Request
        WorkRequestProxy workRequestProxy = getProxyProvider().getWorkRequestProxyForWc(workCompletionEvent);
        // Must free the request
        workRequestProxy.releaseWorkRequest();
        // Status 5 can happen on remote side disconnect, since we have already posted
        // RECV requests for that remote side. We can simply close the remote endpoint
        // at this point.
        if(workCompletionEvent.getStatus() == IbvWC.IbvWcStatus.IBV_WC_WR_FLUSH_ERR.ordinal()){
            ActiveRdmaCommunicator communicator = (ActiveRdmaCommunicator) workRequestProxy.getEndpoint();
            try {
                communicator.close();
            } catch (IOException | InterruptedException e) {
                logger.error("Error in closing endpoint.", e);
            }
        }else{
            logger.error("Error in network request "+ workCompletionEvent.getWr_id()
                    + " of status : " + workCompletionEvent.getStatus());
        }
    }
}
