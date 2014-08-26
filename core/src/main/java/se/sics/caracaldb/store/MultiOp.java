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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map.Entry;
//import java.util.function.Predicate;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.utils.ByteArrayRef;

/**
 *
 * @author lkroll
 */
public class MultiOp {

    public static class Req extends StorageRequest {

        public final ImmutableSet<Condition> conditions;
        public final ImmutableMap<Key, byte[]> successPuts;
        public final ImmutableMap<Key, byte[]> failurePuts;
        public final int versionId;

        public Req(ImmutableSet<Condition> conditions, ImmutableMap<Key, byte[]> successPuts, ImmutableMap<Key, byte[]> failurePuts, int versionId) {
            this.conditions = conditions;
            this.successPuts = successPuts;
            this.failurePuts = failurePuts;
            this.versionId = versionId;
        }

        @Override
        public StorageResponse execute(Persistence store) throws IOException {
            boolean success = true;
            for (Condition c : conditions) {
                ByteArrayRef value = store.get(c.on().getArray());
                if (!c.holds(value)) {
                    success = false;
                    break;
                }
            }
            ImmutableMap<Key, byte[]> puts;
            if (success) {
                puts = successPuts;
            } else {
                puts = failurePuts;
            }
            if (puts.isEmpty()) {
                return new Resp(success, this, null);
            }

            Batch batch = store.createBatch();
            long sizeDiff = 0;
            long numDiff = 0;
            try {
                for (Entry<Key, byte[]> e : puts.entrySet()) {
                    byte[] key = e.getKey().getArray();
                    byte[] value = e.getValue();
                    ByteArrayRef oldValue = store.get(key);
                    if (value == null) {
                        batch.delete(key, versionId);
                        if (oldValue != null) {
                            sizeDiff -= oldValue.length;
                            numDiff--;
                        }
                    } else {
                        batch.put(key, value, versionId);
                        if (oldValue == null) {
                            sizeDiff += value.length;
                            numDiff++;
                        } else {
                            sizeDiff -= oldValue.length - value.length;
                        }
                    }
                }
                store.writeBatch(batch);
                return new Resp(success, this, new Diff(sizeDiff, numDiff));
            } finally {
                batch.close();
            }

        }

    }

    public static class Resp extends StorageResponse {

        public final boolean success;

        public Resp(boolean success, StorageRequest req, Diff diff) {
            super(req, diff);
            this.success = success;
        }

    }

    public static interface Condition extends Serializable {

        public Key on();

        public boolean holds(ByteArrayRef value);
    }

    // public static abstract class ConditionFactory {

    //     public static Condition check(Key k, Predicate<ByteArrayRef> pred) {
    //         return new FunctionalCondition(k, pred);
    //     }
    // }

    public static abstract class SingleKeyCondition implements Condition {

        public final Key k;

        public SingleKeyCondition(Key k) {
            this.k = k;
        }

        @Override
        public Key on() {
            return k;
        }
    }

    // public static class FunctionalCondition extends SingleKeyCondition {

    //     public final Predicate<ByteArrayRef> pred;

    //     public FunctionalCondition(Key k, Predicate<ByteArrayRef> pred) {
    //         super(k);
    //         this.pred = pred;
    //     }

    //     @Override
    //     public boolean holds(ByteArrayRef value) {
    //         return pred.test(value);
    //     }
    // }

    public static class EqualCondition extends SingleKeyCondition {

        public final byte[] oldValue;

        public EqualCondition(Key k, byte[] oldValue) {
            super(k);
            this.oldValue = oldValue;
        }

        @Override
        public boolean holds(ByteArrayRef value) {
            return value.compareTo(oldValue) == 0;
        }

    }
}
