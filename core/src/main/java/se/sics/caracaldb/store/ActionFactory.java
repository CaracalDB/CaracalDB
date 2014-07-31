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

import java.io.Serializable;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.utils.ByteArrayRef;

/**
 *
 * @author lkroll
 */
public class ActionFactory {

    private static final RangeAction NOOP = new Noop();

    public static RangeAction noop() {
        return NOOP;
    }

    public static RangeAction delete() {
        return new Delete();
    }

    public static RangeAction fullDelete() {
        return new FullDelete();
    }

    public static RangeAction writeBack() {
        return new WriteBack();
    }

    public static class Noop implements RangeAction, Serializable {

        @Override
        public void prepare(Persistence store) {
            // ignore
        }

        @Override
        public long process(byte[] key, ByteArrayRef newValue, int versionId) {
            // ignore
            return -1;
        }

        @Override
        public void commit() {
            // ignore
        }

        @Override
        public void abort() {
            // ignore
        }

    }

    public static class Delete implements RangeAction, Serializable {

        private Persistence store;
        private Batch batch;

        public Delete() {
        }

        @Override
        public void prepare(Persistence store) {
            this.store = store;
            batch = store.createBatch();
        }

        @Override
        public long process(byte[] key, ByteArrayRef newValue, int versionId) {
            batch.delete(key, versionId);
            return 0;
        }

        @Override
        public void commit() {
            store.writeBatch(batch);
            batch.close();
        }

        @Override
        public void abort() {
            batch.close();
        }

    }

    public static class FullDelete implements RangeAction, Serializable {

        private Persistence store;
        private Batch batch;

        public FullDelete() {
        }

        @Override
        public void prepare(Persistence store) {
            this.store = store;
            batch = store.createBatch();
        }

        @Override
        public long process(byte[] key, ByteArrayRef newValue, int versionId) {
            batch.replace(key, new ByteArrayRef(0, 0, null));
            return 0;
        }

        @Override
        public void commit() {
            store.writeBatch(batch);
            batch.close();
        }

        @Override
        public void abort() {
            batch.close();
        }

    }

    public static class WriteBack implements RangeAction, Serializable {

        private static final int MAX_BATCH_SIZE = 50; //TODO figure out what a good value is

        private Persistence store;
        private Batch batch;
        private int curBatchSize = 0;

        public WriteBack() {
        }

        @Override
        public void prepare(Persistence store) {
            this.store = store;
            batch = store.createBatch();
        }

        @Override
        public long process(byte[] key, ByteArrayRef newValue, int versionId) {
            if (curBatchSize >= MAX_BATCH_SIZE) {
                store.writeBatch(batch);
                batch.close();
                batch = store.createBatch();
            }
            curBatchSize++;
            if (newValue == null) {
                batch.delete(key, versionId);
                return 0;
            } else {
                batch.put(key, newValue.dereference(), versionId);
            }

            return newValue.length;
        }

        @Override
        public void commit() {
            store.writeBatch(batch);
            batch.close();
        }

        @Override
        public void abort() {
            batch.close();
        }
    }
}
