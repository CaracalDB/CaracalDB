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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.Database;
import se.sics.caracaldb.persistence.StoreIterator;

/**
 * A TreeMap based Database implementation.
 *
 * Note that the close() operation deletes all items!
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class InMemoryDB implements Database {

    private TreeMap<byte[], byte[]> store;

    public InMemoryDB() {
        store = new TreeMap<byte[], byte[]>(Key.COMP);
    }

    @Override
    public String toString() {
        return "InMemoryDb(" + store.size() + " keys)";
    }

    @Override
    public void put(byte[] key, byte[] value) {
        store.put(key, value);
    }

    @Override
    public void delete(byte[] key) {
        store.remove(key);
    }

    @Override
    public byte[] get(byte[] key) {
        return store.get(key);
    }

    @Override
    public Batch createBatch() {
        return new InMemBatch();
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

        @Override
        public void put(byte[] key, byte[] value) {
            ops.add(new PutOp(key, value));
        }

        @Override
        public void delete(byte[] key) {
            ops.add(new DeleteOp(key));
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
        public Entry<byte[], byte[]> peekNext() {
            return currentEntry;
        }

        @Override
        public byte[] peekKey() {
            return currentEntry.getKey();
        }

        @Override
        public byte[] peekValue() {
            return currentEntry.getValue();
        }

        @Override
        public void close() {
            it = null;
            currentEntry = null;
        }
    }
}
