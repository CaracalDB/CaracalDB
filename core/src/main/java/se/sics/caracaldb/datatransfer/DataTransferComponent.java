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
package se.sics.caracaldb.datatransfer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.flow.DataFlow;
import se.sics.caracaldb.store.Store;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
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
    Positive<DataFlow> flow = requires(DataFlow.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    Positive<Store> store = requires(Store.class);

    protected final UUID id;
    protected State state;

    protected Key lastKey;

    // statistics
    protected long dataSent = 0;
    protected long itemsSent = 0;

    DataTransferComponent(UUID id) {
        this.id = id;

        subscribe(statusHandler, transfer);
    }

    Handler<StatusRequest> statusHandler = new Handler<StatusRequest>() {

        @Override
        public void handle(StatusRequest event) {
            trigger(new StatusResponse(event, id, dataSent, itemsSent, state), transfer);
        }
    };

    // internal messages and events
    public static class RetryTimeout extends Timeout {

        public RetryTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    public static class Ack extends TransferMessage {

        public Ack(Address src, Address dst, UUID id) {
            super(src, dst, id);
        }
    }

    protected static byte[] serialise(SortedMap<Key, byte[]> result) throws IOException {
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

    protected static SortedMap<Key, byte[]> deserialise(byte[] data) throws IOException {
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
}
