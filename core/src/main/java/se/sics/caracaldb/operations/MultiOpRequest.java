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
package se.sics.caracaldb.operations;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.UUID;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.store.ActionFactory;
import se.sics.caracaldb.store.MultiOp;

/**
 *
 * @author lkroll
 */
public class MultiOpRequest extends CaracalOp {

    public final ImmutableSet<MultiOp.Condition> conditions;
    public final ImmutableMap<Key, byte[]> successPuts;
    public final ImmutableMap<Key, byte[]> failurePuts;

    public MultiOpRequest(UUID id, ImmutableSet<MultiOp.Condition> conditions, ImmutableMap<Key, byte[]> successPuts, ImmutableMap<Key, byte[]> failurePuts) {
        super(id);
        this.conditions = conditions;
        this.successPuts = successPuts;
        this.failurePuts = failurePuts;
    }

    public boolean isInRange(KeyRange range) {
        for (MultiOp.Condition c : conditions) {
            if (!range.contains(c.on())) {
                return false;
            }
        }
        for (Key k : successPuts.keySet()) {
            if (!range.contains(k)) {
                return false;
            }
        }
        for (Key k : failurePuts.keySet()) {
            if (!range.contains(k)) {
                return false;
            }
        }
        return true;
    }

    public boolean writesTo(Key k) {
        return successPuts.keySet().contains(k) || failurePuts.keySet().contains(k);
    }
    
    public Key anyKey() {
        if (!conditions.isEmpty()) {
            MultiOp.Condition c = conditions.iterator().next();
            return c.on();
        }
        if (!successPuts.isEmpty()) {
            return successPuts.keySet().iterator().next();
        }
        if (!failurePuts.isEmpty()) {
            return failurePuts.keySet().iterator().next();
        }
        return null;
    }

    @Override
    public boolean affectedBy(CaracalOp op) {
        ImmutableSet.Builder<Key> readKeysB = ImmutableSet.builder();
        for (MultiOp.Condition c : conditions) {
            readKeysB.add(c.on());
        }
        ImmutableSet<Key> readKeys = readKeysB.build();
        if (op instanceof PutRequest) {
            PutRequest put = (PutRequest) op;
            return readKeys.contains(put.key);
        }
        if (op instanceof RangeQuery.Request) {
            RangeQuery.Request rqr = (RangeQuery.Request) op;
            if (!(rqr.action instanceof ActionFactory.Noop)) {
                for (Key k : readKeys) {
                    if (rqr.subRange.contains(k)) {
                        return true;
                    }
                }
                return false;
            }
        }
        if (op instanceof MultiOpRequest) {
            MultiOpRequest mor = (MultiOpRequest) op;
            for (Key k : readKeys) {
                    if (mor.writesTo(k)) {
                        return true;
                    }
                }
                return false;
        }
        return false;
    }

}
