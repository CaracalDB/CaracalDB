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

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedInteger;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.util.CustomSerialisers;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class LookupTable {

    public static final int INIT_REP_FACTOR = 3;
    public static final int NUM_VIRT_GROUPS = 256;
    private static final String EMPTY_TXT = "<EMPTY>";
    private static final Random RAND = new Random();
    private static LookupTable INSTANCE = null; // Don't tell anyone about this! (static fields and simulations oO)
    private ArrayList<Address> hosts;
    private ArrayList<Integer[]> replicationGroups;
    private ArrayList<Integer> replicationGroupVersions;
    private LookupGroup[] virtualHostGroups;
    private Long[] virtualHostGroupVersions;
    long versionId = 0;

    private LookupTable() {
        virtualHostGroups = new LookupGroup[NUM_VIRT_GROUPS];
        virtualHostGroupVersions = new Long[NUM_VIRT_GROUPS];

    }

    public int numHosts() {
        return hosts.size();
    }

    public Address getHost(int pos) {
        return hosts.get(pos);
    }

    public int numReplicationGroups() {
        return replicationGroups.size();
    }

    public Address[] getHosts(int replicationGroupId) {
        Integer[] group = replicationGroups.get(replicationGroupId);
        if (group == null) {
            return null;
        }
        Address[] hostAddrs = new Address[group.length];
        for (int i = 0; i < group.length; i++) {
            hostAddrs[i] = getHost(i);
        }
        return hostAddrs;
    }

    public Address[] getResponsibles(Key k) {
        Pair<Key, Integer> rg = virtualHostsGetResponsible(k);
        Integer rgId = rg.getValue1();
        if (rgId == null) {
            return null;
        }

        Integer rgVersion = replicationGroupVersions.get(rgId);
        Address[] group = getVirtualHosts(rgId, rg.getValue0());
        return group;
    }

    public View getView(Key nodeId) {
        Integer rgId = virtualHostsGet(nodeId);
        if (rgId == null) {
            return null;
        }

        Integer rgVersion = replicationGroupVersions.get(rgId);
        Address[] group = getVirtualHosts(rgId, nodeId);
        View view = new View(ImmutableSortedSet.copyOf(group), rgVersion);
        return view;
    }

    public Address[] getVirtualHosts(int replicationGroupId, Key nodeId) {
        Integer[] rGroup = replicationGroups.get(replicationGroupId);
        if (rGroup == null) {
            return null;
        }

        Address[] group = new Address[rGroup.length];
        for (int i = 0; i < rGroup.length; i++) {
            Address hostAdr = getHost(rGroup[i]);
            group[i] = hostAdr.newVirtual(nodeId.getArray());
        }
        return group;
    }

    public KeyRange getResponsibility(Key nodeId) {
        Key succ = virtualHostsGetSuccessor(nodeId);
        if (succ == null) {
            return KeyRange.closed(nodeId).open(Key.INF);
        }
        KeyRange range = KeyRange.closed(nodeId).open(succ);
        return range;
    }

    /**
     * Find all the virtual nodes at a host.
     *
     * More exactly, find the ids of all virtual nodes that are supposed to be
     * at the given host according to the state of the LUT.
     *
     * This is a horribly slow operation. It's meant for bootup, use it later at
     * your own risk.
     *
     * @param host
     * @return
     */
    public Set<Key> getVirtualNodesAt(Address host) {
        // find host id
        int hostId = -1;
        for (ListIterator<Address> it = hosts.listIterator(); it.hasNext();) {
            int pos = it.nextIndex();
            Address adr = it.next();
            if (adr.equals(host)) {
                hostId = pos;
                break;
            }
        }
        if (hostId < 0) {
            return null; // could also throw an exeception...not sure what is nicer
        }
        // find all replication groups for hostId
        TreeSet<Integer> repGroupIds = new TreeSet<Integer>();
        for (ListIterator<Integer[]> it = replicationGroups.listIterator(); it.hasNext();) {
            int pos = it.nextIndex();
            Integer[] group = it.next();
            for (int i = 0; i < group.length; i++) {
                if (hostId == group[i]) {
                    repGroupIds.add(pos);
                    break;
                }
            }
        }
        System.out.println(host + " is in repGroups " + repGroupIds);
        if (repGroupIds.isEmpty()) {
            // just return an empty set.
            // if the host is not part of any replication groups
            // clearly there won't be any VNodes on it
            return new HashSet<Key>();
        }

        // now find all the occurences in the lookup groups
        // this is the most horribly inefficient part^^
        HashSet<Key> nodeSet = new HashSet<Key>();
        for (int i = 0; i < NUM_VIRT_GROUPS; i++) {
            if (!virtualHostGroups[i].isEmpty()) {
                for (Integer rgId : repGroupIds) {
                    nodeSet.addAll(virtualHostGroups[i].getVirtualNodesIn(rgId));
                }
            }
        }
        return nodeSet;
    }

    /**
     * Builds a readable format of the LUT.
     *
     * This is probably not a good idea for large tables. Use for debugging of
     * small sets only.
     *
     * @param sb
     */
    public void printFormat(StringBuilder sb) {
        sb.append("### LookupTable (v");
        sb.append(versionId);
        sb.append(") ### \n \n");

        sb.append("## Hosts ## \n");
        for (ListIterator<Address> it = hosts.listIterator(); it.hasNext();) {
            int pos = it.nextIndex();
            Address adr = it.next();
            sb.append(pos);
            sb.append(". ");
            if (adr == null) {
                sb.append(EMPTY_TXT);
            } else {
                sb.append(adr);
            }
            sb.append('\n');
        }
        sb.append('\n');

        sb.append("## Replication Groups ## \n");
        for (ListIterator<Integer[]> it = replicationGroups.listIterator(); it.hasNext();) {
            int pos = it.nextIndex();
            Integer[] group = it.next();
            sb.append(pos);
            sb.append(". ");
            sb.append("(v");
            sb.append(replicationGroupVersions.get(pos));
            sb.append(") ");
            if (group == null) {
                sb.append(EMPTY_TXT);
            } else {
                sb.append('{');
                for (int i = 0; i < group.length; i++) {
                    sb.append(group[i]);
                    if (i < (group.length - 1)) {
                        sb.append(',');
                    }
                }
                sb.append('}');
                sb.append('\n');
            }
        }
        sb.append('\n');

        sb.append("## Virtual Node Groups ## \n");
        for (int i = 0; i < NUM_VIRT_GROUPS; i++) {

            if (!virtualHostGroups[i].isEmpty()) {
                sb.append("# Group ");
                sb.append(i);
                sb.append(" (v");
                sb.append(virtualHostGroupVersions[i]);
                sb.append(") # \n");
                virtualHostGroups[i].printFormat(sb);
                sb.append('\n');
            }
//            else {
//                sb.append(EMPTY_TXT);
//            }

        }

        sb.append('\n');
        sb.append('\n');
    }

    public byte[] serialise() throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            w.writeLong(versionId);

            // hosts
            w.writeInt(hosts.size());
            for (Address addr : hosts) {
                CustomSerialisers.serialiseAddress(addr, w);
            }

            // replicationgroups
            w.writeInt(replicationGroups.size());
            for (ListIterator<Integer[]> it = replicationGroups.listIterator(); it.hasNext();) {
                int pos = it.nextIndex();
                Integer[] group = it.next();
                Integer version = replicationGroupVersions.get(pos);
                serialiseReplicationGroup(version, group, w);
            }

            // virtualHostGroups
            for (int i = 0; i < NUM_VIRT_GROUPS; i++) {
                w.writeLong(virtualHostGroupVersions[i]);
                byte[] lgbytes = virtualHostGroups[i].serialise();
                w.writeInt(lgbytes.length);
                w.write(lgbytes);
            }

            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static LookupTable deserialise(byte[] bytes) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayInputStream bais = closer.register(new ByteArrayInputStream(bytes));
            DataInputStream r = closer.register(new DataInputStream(bais));

            INSTANCE = new LookupTable();

            INSTANCE.versionId = r.readLong();

            // hosts
            int numHosts = r.readInt();
            INSTANCE.hosts = new ArrayList<Address>(numHosts);
            for (int i = 0; i < numHosts; i++) {
                INSTANCE.hosts.add(CustomSerialisers.deserialiseAddress(r));
            }

            // replicationgroups
            int numRGs = r.readInt();
            INSTANCE.replicationGroups = new ArrayList<Integer[]>(numRGs);
            INSTANCE.replicationGroupVersions = new ArrayList<Integer>(numRGs);
            for (int i = 0; i < numRGs; i++) {
                Pair<Integer, Integer[]> group = deserialiseReplicationGroup(r);
                INSTANCE.replicationGroups.add(group.getValue1());
                INSTANCE.replicationGroupVersions.add(group.getValue0());
            }

            // virtualHostGroups
            for (int i = 0; i < NUM_VIRT_GROUPS; i++) {
                INSTANCE.virtualHostGroupVersions[i] = r.readLong();
                int groupLength = r.readInt();
                byte[] groupBytes = new byte[groupLength];
                if (r.read(groupBytes) != groupLength) {
                    throw new IOException("Incomplete dataset!");
                }
                INSTANCE.virtualHostGroups[i] = LookupGroup.deserialise(groupBytes);
            }


            return INSTANCE;
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static void serialiseReplicationGroup(Integer version, Integer[] group, DataOutputStream w) throws IOException {
        w.writeInt(version);
        byte groupSize = UnsignedBytes.checkedCast(group.length);
        w.writeByte(groupSize);
        for (Integer i : group) {
            w.writeInt(i);
        }
    }

    private static Pair<Integer, Integer[]> deserialiseReplicationGroup(DataInputStream r) throws IOException {
        int version = r.readInt();
        int groupSize = UnsignedBytes.toInt(r.readByte());
        Integer[] group = new Integer[groupSize];
        for (int i = 0; i < groupSize; i++) {
            group[i] = r.readInt();
        }
        return Pair.with(version, group);
    }


    public static LookupTable generateInitial(Set<Address> hosts, int vNodesPerHost) {
        LookupTable lut = new LookupTable();
        lut.generateHosts(hosts);
        lut.generateReplicationGroups(hosts);
        lut.generateInitialVirtuals(vNodesPerHost);

        INSTANCE = lut;

        return lut;
    }

    private void generateHosts(Set<Address> hosts) {
        this.hosts = new ArrayList<Address>(hosts);
    }

    private void generateReplicationGroups(Set<Address> hosts) {
        ArrayList<Integer> dup1, dup2, dup3;
        List<Integer> nats = naturals(hosts.size());
        dup1 = new ArrayList<Integer>(nats);
        dup2 = new ArrayList<Integer>(nats);
        dup3 = new ArrayList<Integer>(nats);

        replicationGroups = new ArrayList<Integer[]>(INIT_REP_FACTOR * hosts.size());
        replicationGroupVersions = new ArrayList<Integer>(INIT_REP_FACTOR * hosts.size());

        for (int n = 0; n < INIT_REP_FACTOR; n++) {
            Collections.shuffle(dup1, RAND);
            Collections.shuffle(dup2, RAND);
            Collections.shuffle(dup3, RAND);
            for (int i = 0; i < hosts.size(); i++) {
                int h1, h2, h3;
                h1 = dup1.get(i);
                h2 = dup2.get(i);
                h3 = dup3.get(i);
                while (h2 == h1) {
                    h2 = RAND.nextInt(hosts.size());
                }
                while ((h3 == h1) || (h3 == h2)) {
                    h3 = RAND.nextInt(hosts.size());
                }
                Integer[] group = new Integer[]{h1, h2, h3};
                int pos = n * hosts.size() + i;
                replicationGroups.add(pos, group);
                replicationGroupVersions.add(pos, 0);
            }
        }
    }

    private void generateInitialVirtuals(int vNodesPerHost) {
        Arrays.fill(virtualHostGroupVersions, 0l);
        for (int i = 0; i < NUM_VIRT_GROUPS; i++) {
            virtualHostGroups[i] = new LookupGroup(Ints.toByteArray(i)[3]);
        }

        // Reserved range from 00 00 00 00 to 00 00 00 01
        virtualHostsPut(new Key(0), 0);
        // Place as many virtual nodes as there are hosts in the system
        // for random (non-schema-aligned) writes (more or less evenly distributed)
        virtualHostsPut(new Key(1), RAND.nextInt(replicationGroups.size()));
        int numVNodes = hosts.size()*vNodesPerHost;
        UnsignedInteger incr = (UnsignedInteger.MAX_VALUE.minus(UnsignedInteger.ONE)).dividedBy(UnsignedInteger.fromIntBits(numVNodes));
        UnsignedInteger last = UnsignedInteger.ONE;
        UnsignedInteger ceiling = UnsignedInteger.MAX_VALUE.minus(incr);
        while (last.compareTo(ceiling) <= 0) {
            last = last.plus(incr);
            virtualHostsPut(new Key(last.intValue()), RAND.nextInt(replicationGroups.size()));
        }
    }

    void virtualHostsPut(Key key, Integer value) {
        int groupId = key.getFirstByte();
        LookupGroup group = virtualHostGroups[groupId];
        group.put(key, value);
    }

    Integer virtualHostsGet(Key key) {
        LookupGroup group = virtualHostGroups[key.getFirstByte()];
        return group.get(key);
    }

    Pair<Key, Integer> virtualHostsGetResponsible(Key key) {
        int groupId = key.getFirstByte();
        LookupGroup keyGroup = virtualHostGroups[groupId];
        while (true) {
            try {
                Pair<Key, Integer> i = keyGroup.getResponsible(key);
                return i;
            } catch (NoResponsibleInGroup e) {
                groupId--;
                if (groupId < 0) {
                    return null;
                }
                keyGroup = virtualHostGroups[groupId];
            }
        }
    }

    Key virtualHostsGetSuccessor(Key key) {
        int groupId = key.getFirstByte();
        LookupGroup keyGroup = virtualHostGroups[groupId];
        while (true) {
            try {
                Key k = keyGroup.getSuccessor(key);
                return k;
            } catch (NoResponsibleInGroup e) {
                groupId++;
                if (groupId >= virtualHostGroups.length) {
                    return null;
                }
                keyGroup = virtualHostGroups[groupId];
            }
        }
    }

    private static List<Integer> naturals(int upTo) {
        ArrayList<Integer> nats = new ArrayList<Integer>(upTo);
        for (int i = 0; i < upTo; i++) {
            nats.add(i);
        }
        return nats;
    }

    public static class NoResponsibleInGroup extends Throwable {

        public static final NoResponsibleInGroup exception = new NoResponsibleInGroup();
    }
}
