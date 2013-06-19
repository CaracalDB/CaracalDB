/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import com.google.common.io.Closer;
import com.google.common.primitives.UnsignedBytes;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.store.Store;
import se.sics.kompics.ChannelFilter;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.ClearToSend;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.RequestToSend;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
abstract class DataTransferComponent extends ComponentDefinition {
    
    public enum State {
        
        INITIALISING,
        TRANSFERRING,
        WAITING,
        DONE;
    }

    Negative<DataTransfer> transfer = provides(DataTransfer.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    Positive<Store> store = requires(Store.class);
    
    
    protected final long id;
    protected State state;
    
    protected Key lastKey;
    
    // statistics
    protected long dataSent = 0;
    protected long itemsSent = 0;
    
    DataTransferComponent(long id) {
        this.id = id;
        
        subscribe(statusHandler, transfer);
    }
    
    Handler<StatusRequest> statusHandler = new Handler<StatusRequest>() {

        @Override
        public void handle(StatusRequest event) {
            trigger(new StatusResponse(event, id, dataSent, itemsSent, state), transfer);
        }
    };

    public abstract static class TransferMessage extends Message {

        public final long id;

        public TransferMessage(Address src, Address dst, long id) {
            super(src, dst);
            this.id = id;
        }
    }
    
    
    
    // internal messages and events

    public static class TransferFilter extends ChannelFilter<TransferMessage, Long> {

        public TransferFilter(Long val) {
            super(TransferMessage.class, val, true);
        }

        @Override
        public Long getValue(TransferMessage event) {
            return event.id;
        }
    }

    public static class RetryTimeout extends Timeout {

        public RetryTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    public static class Ack extends TransferMessage {

        public Ack(Address src, Address dst, long id) {
            super(src, dst, id);
        }
    }

    public static class TransferClearToSend extends ClearToSend {

        public TransferClearToSend(Address src, Address dst, RequestToSend req) {
            super(src, dst, req);
        }
    }

    public static class Complete extends TransferMessage {

        public Complete(Address src, Address dst, long id) {
            super(src, dst, id);
        }
    }

    public static class Data extends TransferMessage {

        public final byte[] data;

        public Data(Address src, Address dst, long id, byte[] data) {
            super(src, dst, id);
            this.data = data;
        }
    }
    
    protected static byte[] serialise(SortedMap<Key, byte[]> result) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            w.writeInt(result.size());
            for (Map.Entry<Key, byte[]> e : result.entrySet()) {
                Key k = e.getKey();
                byte[] val = e.getValue();
                if (k.isByteLength()) {
                    w.writeBoolean(true);
                    w.writeByte(k.getByteKeySize());
                } else {
                    w.writeBoolean(false);
                    w.writeInt(k.getKeySize());
                }
                w.write(k.getArray());
                w.writeInt(val.length);
                w.write(val);
            }

            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }
    
    protected static SortedMap<Key, byte[]> deserialise(byte[] data) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayInputStream bais = closer.register(new ByteArrayInputStream(data));
            DataInputStream r = closer.register(new DataInputStream(bais));

            int size = r.readInt();
            TreeMap<Key, byte[]> map = new TreeMap<Key, byte[]>();
            for (int i = 0; i < size; i++) {
                boolean isByteLength = r.readBoolean();
                int keysize;
                if (isByteLength) {
                    keysize = UnsignedBytes.toInt(r.readByte());
                } else {
                    keysize = r.readInt();
                }
                byte[] keydata = new byte[keysize];
                if (r.read(keydata) != keysize) {
                    throw new IOException("Data seems incomplete.");
                }
                Key k = new Key(keydata);
                int valsize = r.readInt();
                byte[] valdata = new byte[valsize];
                if (r.read(valdata) != valsize) {
                    throw new IOException("Data seems incomplete.");
                }
                map.put(k, valdata);
            }


            return map;
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }
}
