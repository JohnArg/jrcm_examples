package examples.serverdiscovery.common;

import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.serialization.AbstractDataSerializer;

import java.nio.ByteBuffer;

/**
 * Encapsulates RPC message headers.
 */
public class DiscoveryRpcPacketHeaders extends AbstractDataSerializer {

    private static final long serialVersionId = 1L;

    // Headers ---------------------------------------------------------------------------
    private byte messageType;                       // what kind of message? (e.g. request or a response?)
    private int operationType;                      // what type of operation (rpc function) to invoke?
    private long operationId;                       // unique operation identifier - associate request with response
    private int packetNumber;                       // the number of this packet
                                                        // in case a large message is split in multiple packets

    public DiscoveryRpcPacketHeaders(WorkRequestProxy workRequestProxy){
        super(workRequestProxy);
    };

    @Override
    public void writeToWorkRequestBuffer() {
        ByteBuffer buffer = getWorkRequestProxy().getBuffer();
        buffer.putLong(serialVersionId);
        buffer.put(messageType);
        buffer.putInt(operationType);
        buffer.putLong(operationId);
        buffer.putInt(packetNumber);
    }

    @Override
    public void readFromWorkRequestBuffer() throws RpcDataSerializationException {
        ByteBuffer buffer = getWorkRequestProxy().getBuffer();
        // read headers -----------------
        long receivedSerialVersionId = buffer.getLong();
        if(receivedSerialVersionId != serialVersionId){
            throw new RpcDataSerializationException("Serial versions do not match. Local version : "+
                    serialVersionId + ", remote version : " + receivedSerialVersionId + ".");
        }
        messageType = buffer.get();
        operationType = buffer.getInt();
        operationId = buffer.getLong();
        packetNumber = buffer.getInt();
    }

    /* *********************************************************
    *   Getters/Setters
    ********************************************************* */

    public static long getSerialVersionId() {
        return serialVersionId;
    }

    public byte getMessageType() {
        return messageType;
    }

    public int getOperationType() {
        return operationType;
    }

    public long getOperationId() {
        return operationId;
    }

    public int getPacketNumber() {
        return packetNumber;
    }

    // Enable method chaining on the setters ---------------

    public DiscoveryRpcPacketHeaders setMessageType(byte messageType) {
        this.messageType = messageType;
        return this;
    }

    public DiscoveryRpcPacketHeaders setOperationType(int operationType) {
        this.operationType = operationType;
        return this;
    }

    public DiscoveryRpcPacketHeaders setOperationId(long operationId) {
        this.operationId = operationId;
        return this;
    }

    public DiscoveryRpcPacketHeaders setPacketNumber(int packetNumber) {
        this.packetNumber = packetNumber;
        return this;
    }
}