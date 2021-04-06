package examples.serverdiscovery.common.serializers;

import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.serialization.AbstractDataSerializer;

import java.nio.ByteBuffer;

/**
 * A (de)serializer of integers that
 * uses a Work Request buffer to write the data to or read the data from.
 */
public class IntSerializer extends AbstractDataSerializer {

    private static final long serialVersionId = 1L;

    private int value;

    public IntSerializer() {
        super();
    }

    public IntSerializer(WorkRequestProxy workRequestProxy) {
        super(workRequestProxy);
    }

    @Override
    public void writeToWorkRequestBuffer() throws RpcDataSerializationException{
        ByteBuffer buffer = getWorkRequestProxy().getBuffer();
        buffer.putLong(serialVersionId);
        buffer.putInt(value);
        buffer.flip();
    }

    @Override
    public void readFromWorkRequestBuffer() throws RpcDataSerializationException {
        ByteBuffer buffer = getWorkRequestProxy().getBuffer();
        // check the serial version id
        long receivedSerialVersionId = buffer.getLong();
        throwIfSerialVersionInvalid(serialVersionId, receivedSerialVersionId);
        value = buffer.getInt();
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
