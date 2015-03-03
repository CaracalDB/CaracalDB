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
package se.sics.caracaldb.global;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.utils.CustomSerialisers;

/**
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class LookupGroup {

    private TreeMap<Key, Integer> virtualHosts; // Replace later with SILT
    private final byte prefix;

    public LookupGroup(byte prefix) {
        virtualHosts = new TreeMap<Key, Integer>();
        this.prefix = prefix;
    }

    public boolean isEmpty() {
        return virtualHosts.isEmpty();
    }

    public void put(Key key, Integer value) {
        if (value == null) {
            virtualHosts.remove(key);
            return;
        }
        virtualHosts.put(key, value);
    }

    public Integer get(Key key) {
        return virtualHosts.get(key);
    }

    public Pair<Key, Integer> getResponsible(Key key) throws LookupTable.NoResponsibleInGroup {
        Map.Entry<Key, Integer> e = virtualHosts.floorEntry(key);
        if (e == null) {
            throw LookupTable.NoResponsibleInGroup.exception;
        } else {
            return Pair.with(e.getKey(), e.getValue());
        }
    }

    /**
     *
     * @param range
     * @return <nodeMap, endRange> nodeMap can be empty but not null, endRange
     * is a non null range. The pair itself cannot be null
     */
    public Pair<NavigableMap<Key, Integer>, KeyRange> getRangeResponsible(KeyRange range) {
        NavigableMap<Key, Integer> respMap = new TreeMap<Key, Integer>();
        KeyRange endRange;

        Entry<Key, Integer> first = virtualHosts.floorEntry(range.begin);
        if (first != null) {
            respMap.put(first.getKey(), first.getValue());
        }
        NavigableMap<Key, Integer> subMap = virtualHosts.subMap(range.begin, true, range.end, range.endBound.equals(KeyRange.Bound.CLOSED));
        respMap.putAll(subMap);

        Key succ = virtualHosts.ceilingKey(range.end);
        if (succ != null) {
            endRange = KeyRange.EMPTY;
        } else {
            if (subMap.isEmpty()) {
                if (first == null) {
                    endRange = KeyRange.EMPTY;
                } else {
                    endRange = range;
                }
            } else {
                endRange = new KeyRange.KRBuilder(KeyRange.Bound.CLOSED, subMap.lastEntry().getKey()).endFrom(range);
            }
        }
        return Pair.with(subMap, endRange);
    }

    public Key getSuccessor(Key key) throws LookupTable.NoResponsibleInGroup {
        Key succ = virtualHosts.higherKey(key);
        if (succ == null) {
            throw LookupTable.NoResponsibleInGroup.exception;
        }
//        if (succ.equals(key)) {
//            throw LookupTable.NoResponsibleInGroup.exception;
//        }
        return succ;
    }

    public Set<Key> getVirtualNodesIn(Integer replicationGroup) {
        Set<Key> nodeSet = new HashSet<Key>();
        for (Entry<Key, Integer> e : virtualHosts.entrySet()) {
            if (e.getValue().equals(replicationGroup)) {
                nodeSet.add(e.getKey());
            }
        }
        return nodeSet;
    }
    
    public Set<Key>getVirtualNodesInSchema(Key schemaId) {
        Set<Key> nodeSet = new TreeSet<Key>();
        for (Entry<Key, Integer> e : virtualHosts.entrySet()) {
            if (e.getKey().hasPrefix(schemaId)) {
                nodeSet.add(e.getKey());
            }
        }
        return nodeSet;
    }

    public byte[] serialise() {

        ByteBuf buf = Unpooled.buffer();

        buf.writeByte(prefix);
        buf.writeInt(virtualHosts.size());
        for (Map.Entry<Key, Integer> e : virtualHosts.entrySet()) {
            Key k = e.getKey();
            Integer val = e.getValue();
            CustomSerialisers.serialiseKey(k, buf);
            buf.writeInt(val);
        }

        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        buf.release();

        return data;
    }

    public static LookupGroup deserialise(byte[] bytes) {

        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        byte prefix = buf.readByte();
        LookupGroup lg = new LookupGroup(prefix);
        int size = buf.readInt();
        for (int i = 0; i < size; i++) {
            Key k = CustomSerialisers.deserialiseKey(buf);
            Integer val = buf.readInt();
            lg.put(k, val);
        }

        return lg;
    }

    public void printFormat(StringBuilder sb) {
        for (Entry<Key, Integer> e : virtualHosts.entrySet()) {
            sb.append(e.getKey());
            sb.append(" -> ");
            sb.append(e.getValue());
            sb.append('\n');
        }
    }
}
