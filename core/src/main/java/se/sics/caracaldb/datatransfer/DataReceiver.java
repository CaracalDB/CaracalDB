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
import se.sics.caracaldb.flow.DataMessage;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.store.BatchWrite;
import se.sics.caracaldb.store.StorageRequest;
import se.sics.caracaldb.store.StorageResponse;
import se.sics.kompics.Handler;
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
    private final int versionId;
    
    public DataReceiver(DataReceiverInit init) {
        super(init.event.id);
        self = init.event.getDestination();
        source = init.event.getSource();
        versionId = (Integer) init.event.metadata.get("versionId");

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
    Handler<DataMessage> dataHandler = new Handler<DataMessage>() {
        @Override
        public void handle(DataMessage event) {
            if (event.isfinal) {
                trigger(new Completed(id), transfer);
                state = State.DONE;
            }
            if (event.data.length == 0) {
                return;
            }
            SortedMap<Key, byte[]> data = null;
            try {
                data = deserialise(event.data);
                    trigger(new BatchWrite(data, versionId), store);
            } catch (IOException ex) {
                LOG.error("Could not deserialise data. Ignoring message!", ex);
            }
        }
    };
    
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
                for (Entry<Key, byte[]> e : data.entrySet()) {
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
}
