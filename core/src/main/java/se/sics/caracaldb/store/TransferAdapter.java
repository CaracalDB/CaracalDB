/*
 * This file is part of the CaracalDB distributed storage system.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.caracaldb.store;

import com.larskroll.common.ByteArrayRef;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.datatransfer.Data;
import se.sics.caracaldb.datatransfer.DataSink;
import se.sics.caracaldb.datatransfer.DataSource;
import se.sics.caracaldb.flow.ByteArrayChunkCollector;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;

/**
 *
 * @author lkroll
 */
public class TransferAdapter extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(TransferAdapter.class);

    // Ports
    Negative<DataSource> source = provides(DataSource.class);
    Negative<DataSink> sink = provides(DataSink.class);
    Positive<Store> store = requires(Store.class);
    // Instance
    private final int versionId;
    private Key lastKey;
    private final KeyRange range;

    public TransferAdapter(Init init) {
        this.versionId = init.versionId;
        this.range = init.range;

        subscribe(startHandler, control);
        subscribe(responseHandler, store);
        subscribe(requestHandler, source);
        subscribe(dataHandler, sink);
    }

    // Handlers
    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            trigger(new Data.Requirements(1000 * 1000), source);
        }
    };
    Handler<RangeResp> responseHandler = new Handler<RangeResp>() {
        @Override
        public void handle(RangeResp event) {
//            if (state != DataTransferComponent.State.TRANSFERRING) {
//                LOG.warn("Got {} while not transferring. Something is out of place!", event);
//                return;
//            }
            if (event.result.isEmpty()) {
                Data.Reference ref = new Data.Reference(ByteArrayRef.wrap(new byte[0]), ByteArrayChunkCollector.descriptor(0), true);
                trigger(ref, source);
                //trigger(new Completed(id), transfer);
                //state = DataTransferComponent.State.DONE;
                return;
            }
            lastKey = event.result.lastKey();
            try {
                byte[] data = serialise(event.result);
                Data.Reference ref = new Data.Reference(ByteArrayRef.wrap(data), ByteArrayChunkCollector.descriptor(data.length), event.readLimit);
                trigger(ref, source);
            } catch (IOException ex) {
                LOG.error("Could not serialise {}. Ignoring!", event);
            }
//            try {
//                byte[] data = serialise(event.result);
//                DataMessage msg;
//                if (data.length < activeCTS.getQuota()) {
//                    msg = activeCTS.constructFinalMessage(data);
//                    trigger(new Completed(id), transfer);
//                    state = DataTransferComponent.State.DONE;
//                } else { // It might actually still be final, but we can't really figure that out without trying again
//                    msg = activeCTS.constructMessage(data);
//                }
//                trigger(msg, net);
//                dataSent += data.length;
//                itemsSent += event.result.size();
//            } catch (IOException ex) {
//                LOG.error("Could not serialise {}. Ignoring!", event);
//            }
//            ClearToSend next = pendingCTS.poll();
//            if (next != null) {
//                activeCTS = next;
//                requestData(next.getQuota());
//            } else {
//                activeCTS = null;
//                state = DataTransferComponent.State.WAITING;
//            }
        }
    };
    Handler<Data.Request> requestHandler = new Handler<Data.Request>() {

        @Override
        public void handle(Data.Request event) {
            requestData(event.size);
        }
    };
    Handler<Data.Reference> dataHandler = new Handler<Data.Reference>() {

        @Override
        public void handle(Data.Reference event) {
            SortedMap<Key, byte[]> data = null;
            try {
                data = deserialise(event.data.dereference());
                trigger(new BatchWrite(data, versionId), store);
            } catch (IOException ex) {
                LOG.error("Could not deserialise data. Ignoring message!", ex);
            }
        }
    };

    private void requestData(long quota) {
        if (lastKey == null) {
            RangeReq rr = new RangeReq(range, Limit.toBytes(quota), TFFactory.tombstoneFilter(), ActionFactory.noop(), -1);
            rr.setMaxVersionId(versionId);
            trigger(rr, store);
            return;
        }

        KeyRange subRange = KeyRange.open(lastKey).endFrom(range);
        RangeReq rr = new RangeReq(subRange, Limit.toBytes(quota), TFFactory.tombstoneFilter(), ActionFactory.noop(), -1);
        rr.setMaxVersionId(versionId);
        trigger(rr, store);
    }

    private static byte[] serialise(SortedMap<Key, byte[]> result) throws IOException {
        ByteBuf buf = Unpooled.buffer();

        buf.writeInt(result.size());
        for (Map.Entry<Key, byte[]> e : result.entrySet()) {
            Key k = e.getKey();
            byte[] val = e.getValue();
            CustomSerialisers.serialiseKey(k, buf);
            buf.writeInt(val.length);
            buf.writeBytes(val);
        }
        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        buf.release();
        return data;
    }

    private static SortedMap<Key, byte[]> deserialise(byte[] data) throws IOException {
        ByteBuf buf = Unpooled.wrappedBuffer(data);

        int size = buf.readInt();
        TreeMap<Key, byte[]> map = new TreeMap<Key, byte[]>();
        for (int i = 0; i < size; i++) {
            Key k = CustomSerialisers.deserialiseKey(buf);
            int valsize = buf.readInt();
            byte[] valdata = new byte[valsize];
            buf.readBytes(valdata);
            map.put(k, valdata);
        }
        buf.release();

        return map;
    }

    public static class PutBlob extends StorageRequest {

        private final int snapshotId;
        private final SortedMap<Key, byte[]> data;

        public PutBlob(SortedMap<Key, byte[]> data, int snapshotId) {
            this.snapshotId = snapshotId;
            this.data = data;
        }

        @Override
        public StorageResponse execute(Persistence store) {
            Batch b = store.createBatch();
            try {
                for (Map.Entry<Key, byte[]> e : data.entrySet()) {
                    Key k = e.getKey();
                    byte[] val = e.getValue();
                    b.put(k.getArray(), val, snapshotId);
                }
                store.writeBatch(b);
            } finally {
                b.close();
            }
            return null;
        }
    }

    public static class Init extends se.sics.kompics.Init<TransferAdapter> {

        public final int versionId;
        public final KeyRange range;

        public Init(int versionId, KeyRange range) {
            this.versionId = versionId;
            this.range = range;
        }
    }
}
