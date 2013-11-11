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

import com.google.common.io.Closer;
import com.google.common.primitives.UnsignedBytes;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;

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
        Key succ = virtualHosts.ceilingKey(key);
        if (succ == null) {
            throw LookupTable.NoResponsibleInGroup.exception;
        }
        if (succ.equals(key)) {
            throw LookupTable.NoResponsibleInGroup.exception;
        }
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

    public byte[] serialise() throws IOException {

        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            w.writeByte(prefix);
            w.writeInt(virtualHosts.size());
            for (Map.Entry<Key, Integer> e : virtualHosts.entrySet()) {
                Key k = e.getKey();
                Integer val = e.getValue();
                if (k.isByteLength()) {
                    w.writeBoolean(true);
                    w.writeByte(k.getByteKeySize());
                } else {
                    w.writeBoolean(false);
                    w.writeInt(k.getKeySize());
                }
                w.write(k.getArray());
                w.writeInt(val);
            }

            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static LookupGroup deserialise(byte[] bytes) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayInputStream bais = closer.register(new ByteArrayInputStream(bytes));
            DataInputStream r = closer.register(new DataInputStream(bais));

            byte prefix = r.readByte();
            LookupGroup lg = new LookupGroup(prefix);
            int size = r.readInt();
            for (int i = 0; i < size; i++) {
                boolean isByteLength = r.readBoolean();
                int keysize;
                if (isByteLength) {
                    keysize = UnsignedBytes.toInt(r.readByte());
                } else {
                    keysize = r.readInt();
                }
                byte[] keydata = new byte[keysize];
                if (r.read(keydata) != keysize) {
                    throw new IOException("Data seems incomplete.");
                }
                Key k = new Key(keydata);
                Integer val = r.readInt();
                lg.put(k, val);
            }

            return lg;
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
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
