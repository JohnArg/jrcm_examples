package examples.serverdiscovery.common.serializers;

import jarg.rdmarpc.networking.dependencies.netrequests.WorkRequestProxy;
import jarg.rdmarpc.rpc.exception.RpcDataSerializationException;
import jarg.rdmarpc.rpc.serialization.AbstractDataSerializer;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A (de)serializer of a List of {@link InetSocketAddress InetSocketAddresses} that
 * uses a Work Request buffer to write the data to or read the data from.
 */
public class InetSocketAddressListSerializer extends AbstractDataSerializer {

    private static final long serialVersionId = 1L;

    private List<InetSocketAddress> addresses;

    public InetSocketAddressListSerializer() {
        super();
        addresses = new ArrayList<>();
    }

    public InetSocketAddressListSerializer(WorkRequestProxy workRequestProxy){
        super(workRequestProxy);
        addresses = new ArrayList<>();
    }


    @Override
    public void writeToWorkRequestBuffer() throws RpcDataSerializationException{
        ByteBuffer buffer = getWorkRequestProxy().getBuffer();
        // first write the serial version
        buffer.putLong(serialVersionId);
        // then write the list's size
        buffer.putInt(addresses.size());
        // then for every address, put the ip bytes and port
        for(InetSocketAddress address : addresses){
            byte[] addressBytes = address.getAddress().getAddress();
            // specify the number of bytes of the address
            buffer.putInt(addressBytes.length);
            // now put the address bytes
            buffer.put(addressBytes);
            // and finally put the port number
            buffer.putInt(address.getPort());
        }
    }

    @Override
    public void readFromWorkRequestBuffer() throws RpcDataSerializationException {
        ByteBuffer buffer = getWorkRequestProxy().getBuffer();
        // check the serial version id
        long receivedSerialVersionId = buffer.getLong();
        throwIfSerialVersionInvalid(serialVersionId, receivedSerialVersionId);
        // read a list of addresses from the buffer
        int listSize = buffer.getInt();
        int addressBytesSize;

        for(int i=0; i<listSize; i++){
            // read the address bytes
            addressBytesSize = buffer.getInt();
            byte[] addressBytes = new byte[addressBytesSize];
            buffer.get(addressBytes);
            try {
                // create a new InetAddress from the bytes
                InetAddress ipAddress = InetAddress.getByAddress(addressBytes);
                // get the port too
                int port = buffer.getInt();
                // add the new address
                addresses.add(new InetSocketAddress(ipAddress, port));
            } catch (UnknownHostException e) {
                throw new RpcDataSerializationException("Cannot deserialize IP address and port.", e);
            }
        }
    }

    /* *********************************************************
     *   Getters/Setters
     ********************************************************* */

    public List<InetSocketAddress> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<InetSocketAddress> addressesSet){
        addresses = new ArrayList<>(addressesSet);
    }

}
