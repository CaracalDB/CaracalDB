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
import java.util.HashMap;
import java.util.UUID;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.store.ActionFactory;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.MultiOp.Condition;
import se.sics.caracaldb.store.MultiOp.EqualCondition;
import se.sics.caracaldb.store.TFFactory;

/**
 *
 * @author lkroll
 */
public abstract class OpUtil {
    /**
     * Check and Set operation.
     * 
     * If value@key == oldValue set value@key = newValue.
     * 
     * @param k
     * @param oldValue
     * @param newValue
     * @return 
     */
    public static MultiOpRequest cas(UUID id, Key key, byte[] oldValue, byte[] newValue) {
        Condition c = new EqualCondition(key, oldValue);
        return new MultiOpRequest(id, ImmutableSet.of(c), ImmutableMap.of(key, newValue), ImmutableMap.copyOf(new HashMap<Key, byte[]>())); // java6 is soooooo baaaadd -.-
    }
    /**
     * Puts value if there's no value associated with key currently.
     * 
     * @param id
     * @param key
     * @param value
     * @return 
     */
    public static MultiOpRequest putIfAbsent(UUID id, Key key, byte[] value) {
        Condition c = new EqualCondition(key, null);
        return new MultiOpRequest(id, ImmutableSet.of(c), ImmutableMap.of(key, value), ImmutableMap.copyOf(new HashMap<Key, byte[]>())); // see above
    }
    
    /**
     * Appends valueAddition to the value with key if key exists.
     * 
     * The Response contains the new value if the key existed or is empty if the key did not exist.
     * 
     * @param id
     * @param key
     * @param valueAddition
     * @return 
     */
    public static RangeQuery.Request append(UUID id, Key key, byte[] valueAddition) {
        KeyRange range = KeyRange.key(key);
        return new RangeQuery.Request(id, range, Limit.toItems(1), new TFFactory.Append(valueAddition), ActionFactory.writeBack(), RangeQuery.Type.SEQUENTIAL);
    }
}
