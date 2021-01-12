package examples.serverdiscovery.common;


import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.packets.AbstractRpcPacket;
import jarg.rdmarpc.rpc.serialization.RpcDataSerializer;

import java.nio.ByteBuffer;

/**
 * Encapsulates RPC messages.
 */
public class DiscoveryRpcPacket extends AbstractRpcPacket {

    private DiscoveryRpcPacketHeaders packetHeaders;             // the headers of the packet

    public DiscoveryRpcPacket(WorkRequestProxy workRequestProxy){
        super(workRequestProxy);
        this.packetHeaders = new DiscoveryRpcPacketHeaders(workRequestProxy);
    }

    public DiscoveryRpcPacket(DiscoveryRpcPacketHeaders packetHeaders, WorkRequestProxy workRequest){
        super(workRequest);
        this.packetHeaders = packetHeaders;
    }

    /* *********************************************************
     *   Write/Read packet data to/from a buffer.
     ********************************************************* */

    @Override
    public void writeToWorkRequestBuffer(RpcDataSerializer payloadSerializer) throws RpcDataSerializationException{
        ByteBuffer packetBuffer = getWorkRequestProxy().getBuffer();
        // write the headers first
        packetHeaders.writeToWorkRequestBuffer();
        // write the payload next, if there is one
        if(payloadSerializer != null){
            payloadSerializer.writeToWorkRequestBuffer();
        }
        // prepare buffer for reading
        packetBuffer.flip();
    }

    @Override
    public void readHeadersFromWorkRequestBuffer() throws RpcDataSerializationException{
        packetHeaders.readFromWorkRequestBuffer();
    }

    /* *********************************************************
     *   Getters/Setters
     ********************************************************* */

    public DiscoveryRpcPacketHeaders getPacketHeaders() {
        return packetHeaders;
    }
}
