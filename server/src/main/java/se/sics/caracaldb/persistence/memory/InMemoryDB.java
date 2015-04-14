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
package se.sics.caracaldb.persistence.memory;

import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.MultiVersionUtil;
import se.sics.caracaldb.persistence.StoreIterator;
import se.sics.caracaldb.persistence.VNodeLevelDB;
import com.larskroll.common.ByteArrayRef;

/**
 * A TreeMap based Database implementation.
 *
 * Note that the close() operation deletes all items!
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class InMemoryDB extends VNodeLevelDB {

    private TreeMap<byte[], byte[]> store;

    public InMemoryDB(Config conf) {
        super(conf);
        store = new TreeMap<byte[], byte[]>(Key.COMP);
    }

    @Override
    public String toString() {
        return "InMemoryDb(" + store.size() + " keys)";
    }

    @Override
    public void put(byte[] key, byte[] value, int version) {
        SortedMap<Integer, ByteArrayRef> versions = getAllVersions(key);
        versions.put(version, new ByteArrayRef(0, value.length, value));
        store.put(key, MultiVersionUtil.pack(versions));
    }

    @Override
    public void delete(byte[] key, int version) {
        SortedMap<Integer, ByteArrayRef> versions = getAllVersions(key);
        if (versions.isEmpty()) {
            return; // nothing to do
        }
        versions.put(version, new ByteArrayRef(0, 0, null));
        store.put(key, MultiVersionUtil.pack(versions));
    }

    @Override
    public ByteArrayRef get(byte[] key) {
        SortedMap<Integer, ByteArrayRef> versions = getAllVersions(key);
        if (versions.isEmpty()) {
            return null;
        }
        return versions.get(versions.firstKey());
    }

    @Override
    public SortedMap<Integer, ByteArrayRef> getAllVersions(byte[] key) {
        byte[] data = store.get(key);
        SortedMap<Integer, ByteArrayRef> versions = MultiVersionUtil.unpack(data);
        return versions;
    }

    @Override
    public Batch createBatch() {
        return new InMemBatch(this);
    }

    @Override
    public void writeBatch(Batch b) {
        InMemBatch imb = (InMemBatch) b;
        // If this cast doesn't work, someone is doing something seriously wrong
        for (Operation op : imb.ops) {
            op.execute(store);
        }
    }

    @Override
    public StoreIterator iterator() {
        return new NavigableMapIterator(store.entrySet().iterator());
    }

    @Override
    public StoreIterator iterator(byte[] startKey) {
        return new NavigableMapIterator(store.tailMap(startKey, true).entrySet().iterator());
    }

    @Override
    public void replace(byte[] key, ByteArrayRef value) {
        store.put(key, value.getBackingArray());
    }

    @Override
    public int deleteVersions(byte[] key, int version) {
        SortedMap<Integer, ByteArrayRef> versions = getAllVersions(key);
        if (versions.isEmpty()) {
            return 0; // nothing to do
        }
        SortedMap<Integer, ByteArrayRef> newVersions = versions.headMap(version);
        if (newVersions.isEmpty()) { // always retain the newest value
            newVersions.put(versions.firstKey(), versions.get(versions.firstKey()));
        }
        byte[] newData = MultiVersionUtil.pack(newVersions);
        store.put(key, newData);
        if (newData == null) {
            return 0;
        }
        return newData.length;
    }

    @Override
    public byte[] getRaw(byte[] key) {
        return store.get(key);
    }

    @Override
    public void close() {
        if (store != null) {
            store.clear();
            store = null;
        }
    }

    private static abstract class Operation {

        final byte[] key;

        Operation(byte[] key) {
            this.key = key;
        }

        abstract void execute(TreeMap<byte[], byte[]> store);
    }

    private static class InMemBatch implements Batch {

        List<Operation> ops = new LinkedList<Operation>();
        InMemoryDB db;

        InMemBatch(InMemoryDB db) {
            this.db = db;
        }

        @Override
        public void put(byte[] key, byte[] value, int version) {
            SortedMap<Integer, ByteArrayRef> versions = db.getAllVersions(key);
            versions.put(version, new ByteArrayRef(0, value.length, value));
            ops.add(new PutOp(key, MultiVersionUtil.pack(versions)));
        }

        @Override
        public void delete(byte[] key, int version) {
            SortedMap<Integer, ByteArrayRef> versions = db.getAllVersions(key);
            if (versions.isEmpty()) {
                return; // nothing to do
            }
            versions.put(version, new ByteArrayRef(0, 0, null));
            ops.add(new PutOp(key, MultiVersionUtil.pack(versions)));
        }

        @Override
        public void replace(byte[] key, ByteArrayRef value) {
            if (value.length == 0) {
                ops.add(new DeleteOp(key));
            } else {
                ops.add(new PutOp(key, value.getBackingArray()));
            }
        }

        @Override
        public int deleteVersions(byte[] key, int version) {
            SortedMap<Integer, ByteArrayRef> versions = db.getAllVersions(key);
            if (versions.isEmpty()) {
                return 0; // nothing to do
            }
            SortedMap<Integer, ByteArrayRef> newVersions = versions.headMap(version);
            if (newVersions.isEmpty()) { // always retain the newest value
                newVersions.put(versions.firstKey(), versions.get(versions.firstKey()));
            }
            byte[] newData = MultiVersionUtil.pack(newVersions);
            ops.add(new PutOp(key, newData));
            if (newData == null) {
                return 0;
            }
            return newData.length;
        }

        @Override
        public void close() {
            ops.clear();
            ops = null;
        }

        private static class PutOp extends Operation {

            final byte[] value;

            PutOp(byte[] key, byte[] value) {
                super(key);
                this.value = value;
            }

            @Override
            void execute(TreeMap<byte[], byte[]> store) {
                store.put(key, value);
            }
        }

        private static class DeleteOp extends Operation {

            DeleteOp(byte[] key) {
                super(key);
            }

            @Override
            void execute(TreeMap<byte[], byte[]> store) {
                store.remove(key);
            }
        }
    }

    private static class NavigableMapIterator implements StoreIterator {

        private Iterator<Entry<byte[], byte[]>> it;
        private Entry<byte[], byte[]> currentEntry;

        NavigableMapIterator(Iterator<Entry<byte[], byte[]>> it) {
            this.it = it;
            if (it.hasNext()) {
                currentEntry = it.next();
            }
        }

        @Override
        public boolean hasNext() {
            return currentEntry != null;
        }

        @Override
        public void next() {
            if (it.hasNext()) {
                currentEntry = it.next();
            } else {
                currentEntry = null;
            }
        }

        @Override
        public byte[] peekKey() {
            return currentEntry.getKey();
        }

        @Override
        public ByteArrayRef peekValue() {
            SortedMap<Integer, ByteArrayRef> versions = peekAllValues();
            if (versions.isEmpty()) {
                return null;
            }
            return versions.get(versions.firstKey());
        }

        @Override
        public SortedMap<Integer, ByteArrayRef> peekAllValues() {
            return MultiVersionUtil.unpack(peekRaw());
        }

        @Override
        public byte[] peekRaw() {
            return currentEntry.getValue();
        }

        @Override
        public void close() {
            it = null;
            currentEntry = null;
        }
    }
}
