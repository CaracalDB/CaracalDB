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

import java.io.IOException;
import java.util.Map.Entry;
import java.util.SortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.store.BatchWrite;
import se.sics.caracaldb.store.StorageRequest;
import se.sics.kompics.Handler;
import se.sics.kompics.Response;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DataReceiver extends DataTransferComponent {

    private static final Logger LOG = LoggerFactory.getLogger(DataReceiver.class);
    private final Address self;
    private final Address source;
    private final boolean force;

    public DataReceiver(DataReceiverInit init) {
        super(init.event.id);
        self = init.event.getDestination();
        source = init.event.getSource();
        force = init.force;

        // subscriptions

        subscribe(startHandler, control);
        subscribe(dataHandler, net);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            trigger(new Ack(self, source, id), net);
            state = State.WAITING;
        }
    };
    Handler<Data> dataHandler = new Handler<Data>() {
        @Override
        public void handle(Data event) {
            SortedMap<Key, byte[]> data = null;
            try {
                data = deserialise(event.data);
                if (force) {
                    trigger(new BatchWrite(data), store);
                } else {
                    trigger(new PutIfNotExists(data), store);
                }
            } catch (IOException ex) {
                LOG.error("Could not deserialise data. Ignoring message!", ex);
            }
        }
    };
    Handler<Complete> completeHandler = new Handler<Complete>() {
        @Override
        public void handle(Complete event) {
            trigger(new Completed(id), transfer);
            state = State.DONE;
        }
    };

    public static class PutIfNotExists extends StorageRequest {

        private final SortedMap<Key, byte[]> data;

        public PutIfNotExists(SortedMap<Key, byte[]> data) {
            this.data = data;
        }

        @Override
        public Response execute(Persistence store) {
            for (Entry<Key, byte[]> e : data.entrySet()) {
                Key k = e.getKey();
                byte[] val = e.getValue();
                byte[] oldVal = store.get(e.getKey().getArray());
                if (oldVal == null) {
                    store.put(k.getArray(), val);
                }
            }

            return null;
        }
    }
}
