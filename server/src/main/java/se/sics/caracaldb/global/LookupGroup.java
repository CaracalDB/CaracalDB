/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
import java.util.Set;
import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
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
