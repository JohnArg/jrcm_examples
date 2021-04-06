package examples.serverdiscovery.common.serializers;

import jarg.jrcm.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.jrcm.rpc.exception.RpcDataSerializationException;
import jarg.jrcm.rpc.serialization.AbstractDataSerializer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

/**
 * A (de)serializer of a List of {@link InetAddress InetAddresses} that
 * uses a Work Request buffer to write the data to or read the data from.
 */
public class InetAddressSerializer extends AbstractDataSerializer {

    private static final long serialVersionId = 1L;

    private InetAddress address;

    public InetAddressSerializer() {
    }

    public InetAddressSerializer(WorkRequestProxy workRequestProxy) {
        super(workRequestProxy);
    }

    @Override
    public void writeToWorkRequestBuffer() throws RpcDataSerializationException{
        ByteBuffer buffer = getWorkRequestProxy().getBuffer();
        // first write the serial version
        buffer.putLong(serialVersionId);
        // serialize the address
        byte[] addressBytes = address.getAddress();
        // specify the number of bytes of the address
        buffer.putInt(addressBytes.length);
        // now put the address bytes
        buffer.put(addressBytes);
        buffer.flip();
    }

    @Override
    public void readFromWorkRequestBuffer() throws RpcDataSerializationException {
        ByteBuffer buffer = getWorkRequestProxy().getBuffer();
        // check the serial version id
        long receivedSerialVersionId = buffer.getLong();
        throwIfSerialVersionInvalid(serialVersionId, receivedSerialVersionId);
        // get address bytes size first
        int addressBytesSize = buffer.getInt();
        // now read that many bytes of an address
        byte[] addressBytes = new byte[addressBytesSize];
        buffer.get(addressBytes);
        try {
            address = InetAddress.getByAddress(addressBytes);
        } catch (UnknownHostException e) {
            throw new RpcDataSerializationException("Cannot deserialize IP address", e);
        }
    }

    /* *********************************************************
     *   Getters/Setters
     ********************************************************* */

    public InetAddress getAddress() {
        return address;
    }

    public void setAddress(InetAddress address) {
        this.address = address;
    }
}
