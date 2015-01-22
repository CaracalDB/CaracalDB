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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedInts;
import com.google.common.primitives.UnsignedLong;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.bootstrap.BootstrapServer;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.utils.J6;
import se.sics.kompics.address.Address;
import se.sics.kompics.address.IdUtils;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;

/**
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class LookupTable {

    public static final int NUM_VIRT_GROUPS = 256;
    public static final Key RESERVED_PREFIX = new Key(0); // (00 00 00 00)
    public static final Key RESERVED_START = Key.ZERO_KEY; // (00)
    public static final Key RESERVED_END = new Key(1); // (00 00 00 01)
    public static final Key RESERVED_HEARTBEATS = RESERVED_PREFIX.append(new byte[]{0, 0, 0, 1}).get(); // (00 00 00 00 00 00 00 01)
    public static final Key RESERVED_LUTUPDATES = RESERVED_PREFIX.append(new byte[]{0, 0, 0, 2}).get(); // (00 00 00 00 00 00 00 02)
    private static final String EMPTY_TXT = "<EMPTY>";
    static final Random RAND = new Random();
    private static LookupTable INSTANCE = null; // Don't tell anyone about this! (static fields and simulations oO)
    private SchemaData schemas;
    private ArrayList<Address> hosts;
    private ArrayList<Integer[]> replicationSets;
    private ArrayList<Integer> replicationSetVersions;
    private LookupGroup[] virtualHostGroups;
    private Long[] virtualHostGroupVersions;
    long versionId = 0;
    private int scatterWidth = -1; // don't forget to set this properly from the config!
    private int masterRepSize = -1; // set from schema information for heartbeats

    private LookupTable() {
        schemas = new SchemaData();
        virtualHostGroups = new LookupGroup[NUM_VIRT_GROUPS];
        virtualHostGroupVersions = new Long[NUM_VIRT_GROUPS];
        Arrays.fill(virtualHostGroupVersions, 0l);
        for (int i = 0; i < NUM_VIRT_GROUPS; i++) {
            virtualHostGroups[i] = new LookupGroup(Ints.toByteArray(i)[3]);
        }
    }

    public int getScatterWidth() {
        return this.scatterWidth;
    }

    public int getMasterReplicationGroupSize() {
        return this.masterRepSize;
    }

    public int numHosts() {
        return hosts.size();
    }

    public Address getHost(int pos) {
        return hosts.get(pos);
    }

    public int numReplicationSets() {
        return replicationSets.size();
    }

    public Address[] getHosts(int replicationGroupId) {
        Integer[] group = replicationSets.get(replicationGroupId);
        if (group == null) {
            return null;
        }
        Address[] hostAddrs = new Address[group.length];
        for (int i = 0; i < group.length; i++) {
            hostAddrs[i] = getHost(group[i]);
        }
        return hostAddrs;
    }

    public Address[] getResponsibles(Key k) throws NoSuchSchemaException {
        Pair<Key, Integer> rg = virtualHostsGetResponsible(k);
        Integer rgId = rg.getValue1();
        if (rgId == null) {
            return null;
        }

        //Integer rgVersion = replicationSetVersions.get(rgId);
        Address[] group = getVirtualHosts(rgId, rg.getValue0());
        return group;
    }

    public Integer[] getResponsibleIds(Key k) throws NoSuchSchemaException {
        Pair<Key, Integer> rg = virtualHostsGetResponsible(k);
        if (rg == null) {
            return null;
        }
        Integer rgId = rg.getValue1();
        if (rgId == null) {
            return null;
        }
        return replicationSets.get(rgId);
    }

    /**
     *
     * @param range
     * @return a map of sub-keyRanges and their respective replication group.
     * this map cannot be null, but it can be empty if the range itself is
     * emptyRange else it should contain at least one replicationGroup
     * @throws se.sics.caracaldb.global.LookupTable.BrokenLut
     */
    public NavigableMap<KeyRange, Address[]> getAllResponsibles(KeyRange range) throws BrokenLut, NoSuchSchemaException {
        TreeMap<KeyRange, Address[]> result = new TreeMap<KeyRange, Address[]>();
        if (range.equals(KeyRange.EMPTY)) {
            return result;
        }

        Pair<Integer, Pair<Key, Integer>> rangeInfo = virtualHostsGetResponsibleWithGid(range.begin);
        if (rangeInfo == null) {
            throw BrokenLut.exception;
        }

        Integer lgId = rangeInfo.getValue0();
        NavigableMap<Key, Integer> keyMap = new TreeMap<Key, Integer>();
        KeyRange endRange = range;
        do {
            LookupGroup lg = virtualHostGroups[lgId];
            Pair<NavigableMap<Key, Integer>, KeyRange> subSpace = lg.getRangeResponsible(endRange);
            keyMap.putAll(subSpace.getValue0());
            endRange = subSpace.getValue1();
        } while (!endRange.equals(KeyRange.EMPTY));

        KeyRange.KRBuilder krb;
        Address[] group;
        Iterator<Entry<Key, Integer>> it = keyMap.entrySet().iterator();
        if (it.hasNext()) {
            KeyRange subRange;
            Entry<Key, Integer> entry = it.next();
            krb = new KeyRange.KRBuilder(range.beginBound, range.begin);
            group = getVirtualHosts(entry.getValue(), entry.getKey());

            while (it.hasNext()) {
                entry = it.next();
                subRange = krb.open(entry.getKey());
                result.put(subRange, group);

                krb = new KeyRange.KRBuilder(KeyRange.Bound.CLOSED, entry.getKey());
                group = getVirtualHosts(entry.getValue(), entry.getKey());
            }

            subRange = krb.endFrom(range);
            result.put(subRange, group);
        }
        return result;
    }

    public Pair<KeyRange, Address[]> getFirstResponsibles(KeyRange range) throws BrokenLut, NoSuchSchemaException {
        if (range.equals(KeyRange.EMPTY)) {
            return null;
        }

        Pair<Integer, Pair<Key, Integer>> rangeInfo = virtualHostsGetResponsibleWithGid(range.begin);
        Key rgKey = rangeInfo.getValue1().getValue0();
        Integer rgId = rangeInfo.getValue1().getValue1();
        if (rangeInfo == null) {
            throw BrokenLut.exception;
        }
        Key endR = virtualHostsGetSuccessor(rgKey);
        KeyRange firstRange;
        if (endR == null || endR.compareTo(range.end) >= 0) {
            firstRange = range;
        } else {
            firstRange = KeyRange.startFrom(range).open(endR);
        }

        Address[] group = getVirtualHosts(rgId, rgKey);
        return Pair.with(firstRange, group);
    }

    public View getView(Key nodeId) {
        Integer rgId = virtualHostsGet(nodeId);
        if (rgId == null) {
            return null;
        }

        Integer rgVersion = replicationSetVersions.get(rgId);
        Address[] group = getVirtualHosts(rgId, nodeId);
        View view = new View(ImmutableSortedSet.copyOf(group), rgVersion);
        return view;
    }

    public Address[] getVirtualHosts(int replicationGroupId, Key nodeId) {
        Integer[] rGroup = replicationSets.get(replicationGroupId);
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

    public KeyRange getResponsibility(Key nodeId) throws NoSuchSchemaException {
        Key succ = virtualHostsGetSuccessor(nodeId);
        //System.out.println("For node " + nodeId + " the successor is " + succ);
        if (succ == null) {
            ByteBuffer schemaId = getSchemaId(nodeId);
            Key schemaKey = new Key(schemaId);
            return KeyRange.closed(nodeId).open(schemaKey.inc()); // until the end of the schema
        }
        KeyRange range = KeyRange.closed(nodeId).open(succ);
        return range;
    }

    public SchemaData.SingleSchema getSchema(Key k) throws NoSuchSchemaException {
        ByteBuffer schemaId = getSchemaId(k);
        if (schemaId != null) {
            return new SchemaData.SingleSchema(schemaId, schemas.schemaNames.get(schemaId), schemas.metaData.get(schemaId));
        }
        return null;
    }

    /**
     * Find all the virtual nodes at a host.
     * <p>
     * More exactly, find the ids of all virtual nodes that are supposed to be
     * at the given host according to the state of the LUT.
     * <p>
     * This is a horribly slow operation. It's meant for bootup, use it later at
     * your own risk.
     * <p>
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
        for (ListIterator<Integer[]> it = replicationSets.listIterator(); it.hasNext();) {
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

    public Set<Key> getVirtualNodesFor(Integer replicationSetId) {
        HashSet<Key> nodeSet = new HashSet<Key>();
        for (int i = 0; i < NUM_VIRT_GROUPS; i++) {
            if (!virtualHostGroups[i].isEmpty()) {
                nodeSet.addAll(virtualHostGroups[i].getVirtualNodesIn(replicationSetId));
            }
        }
        return nodeSet;
    }

    public Set<Key> getVirtualNodesInSchema(Key schemaId) {
        Set<Key> nodeSet = new TreeSet<Key>();
        for (int i = 0; i < NUM_VIRT_GROUPS; i++) {
            if (!virtualHostGroups[i].isEmpty()) {
                nodeSet.addAll(virtualHostGroups[i].getVirtualNodesInSchema(schemaId));
            }
        }
        return nodeSet;
    }

    /**
     * Builds a readable format of the LUT.
     * <p>
     * This is probably not a good idea for large tables. Use for debugging of
     * small sets only.
     * <p>
     * @param sb
     */
    public void printFormat(StringBuilder sb) {
        sb.append("### LookupTable (v");
        sb.append(versionId);
        sb.append(") ### \n \n");

        sb.append("## Schemas ## \n");
        for (Entry<String, ByteBuffer> e : schemas.schemaIDs.entrySet()) {
            String name = e.getKey();
            byte[] id = e.getValue().array();
            sb.append(IdUtils.printFormat(id));
            sb.append(" : ");
            sb.append(schemas.schemaInfo(name));
            sb.append('\n');
        }
        sb.append('\n');

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
        for (ListIterator<Integer[]> it = replicationSets.listIterator(); it.hasNext();) {
            int pos = it.nextIndex();
            Integer[] group = it.next();
            sb.append(pos);
            sb.append(". ");
            sb.append("(v");
            sb.append(replicationSetVersions.get(pos));
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

    public byte[] serialise() {
        ByteBuf buf = Unpooled.buffer();

        buf.writeLong(versionId);
        buf.writeInt(scatterWidth);

        // schemas
        schemas.serialise(buf);

        // hosts
        buf.writeInt(hosts.size());
        for (Address addr : hosts) {
            SpecialSerializers.AddressSerializer.INSTANCE.toBinary(addr, buf);
        }

        // replicationgroups
        buf.writeInt(replicationSets.size());
        for (ListIterator<Integer[]> it = replicationSets.listIterator(); it.hasNext();) {
            int pos = it.nextIndex();
            Integer[] group = it.next();
            Integer version = replicationSetVersions.get(pos);
            serialiseReplicationSet(version, group, buf);
        }

        // virtualHostGroups
        for (int i = 0; i < NUM_VIRT_GROUPS; i++) {
            buf.writeLong(virtualHostGroupVersions[i]);
            byte[] lgbytes = virtualHostGroups[i].serialise();
            buf.writeInt(lgbytes.length);
            buf.writeBytes(lgbytes);
        }

        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        buf.release();

        return data;
    }

    public static LookupTable deserialise(byte[] bytes) throws IOException {

        ByteBuf buf = Unpooled.wrappedBuffer(bytes);

        INSTANCE = new LookupTable();

        INSTANCE.versionId = buf.readLong();
        INSTANCE.scatterWidth = buf.readInt();

        // schemas
        INSTANCE.schemas = SchemaData.deserialise(buf);

        // hosts
        int numHosts = buf.readInt();
        INSTANCE.hosts = new ArrayList<Address>(numHosts);
        for (int i = 0; i < numHosts; i++) {
            Address addr = (Address) SpecialSerializers.AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            INSTANCE.hosts.add(addr);
        }

        // replicationgroups
        int numRGs = buf.readInt();
        INSTANCE.replicationSets = new ArrayList<Integer[]>(numRGs);
        INSTANCE.replicationSetVersions = new ArrayList<Integer>(numRGs);
        for (int i = 0; i < numRGs; i++) {
            Pair<Integer, Integer[]> group = deserialiseReplicationGroup(buf);
            INSTANCE.replicationSets.add(group.getValue1());
            INSTANCE.replicationSetVersions.add(group.getValue0());
        }

        // virtualHostGroups
        for (int i = 0; i < NUM_VIRT_GROUPS; i++) {
            INSTANCE.virtualHostGroupVersions[i] = buf.readLong();
            int groupLength = buf.readInt();
            byte[] groupBytes = new byte[groupLength];
            buf.readBytes(groupBytes);
            INSTANCE.virtualHostGroups[i] = LookupGroup.deserialise(groupBytes);
        }

        return INSTANCE;
    }

    private static void serialiseReplicationSet(Integer version, Integer[] group, ByteBuf buf) {
        buf.writeInt(version);
        byte groupSize = UnsignedBytes.checkedCast(group.length);
        buf.writeByte(groupSize);
        for (Integer i : group) {
            buf.writeInt(i);
        }
    }

    private static Pair<Integer, Integer[]> deserialiseReplicationGroup(ByteBuf buf) {
        int version = buf.readInt();
        int groupSize = UnsignedBytes.toInt(buf.readByte());
        Integer[] group = new Integer[groupSize];
        for (int i = 0; i < groupSize; i++) {
            group[i] = buf.readInt();
        }
        return Pair.with(version, group);
    }

    public static LookupTable generateInitial(Set<Address> hosts, Configuration config, Address self) {
        LookupTable lut = new LookupTable();
        lut.scatterWidth = config.getInt("caracal.scatterWidth");
        lut.generateHosts(hosts);
        lut.loadSchemas(config);
        // Generate initial replication sets for all requested sizes
        Set<Integer> rfSizes = lut.findReplicationSetSizes();
        lut.replicationSets = new ArrayList<Integer[]>();
        for (Integer rf : rfSizes) {
            ArrayList<Integer[]> rss = lut.generateReplicationSetsOfSize(hosts, rf);
            lut.replicationSets.addAll(rss);
        }
        lut.fixRepSetsToIncludeBootstrapNodeInMasterGroup(self); // This is optional but a simple optimisation
        // Set all versions to 0
        lut.replicationSetVersions = new ArrayList<Integer>(lut.replicationSets.size());
        for (int i = 0; i < lut.replicationSets.size(); i++) {
            lut.replicationSetVersions.add(i, 0);
        }
        for (ByteBuffer schemaId : lut.schemas.metaData.keySet()) {
            lut.generateInitialVirtuals(schemaId);
        }

        INSTANCE = lut;

        return lut;
    }

    private void generateHosts(Set<Address> hosts) {
        this.hosts = new ArrayList<Address>(hosts);
    }

    private void loadSchemas(Configuration config) {
        schemas = SchemaReader.importSchemas(config);
    }

    private Set<Integer> findReplicationSetSizes() {
        Set<Integer> sizes = new TreeSet<Integer>();
        for (Entry<ByteBuffer, ImmutableMap<String, String>> e : schemas.metaData.entrySet()) {
            ByteBuffer id = e.getKey();
            ImmutableMap<String, String> meta = e.getValue();
            String rfS = J6.orDefault(meta.get("rfactor"), "3");
            Integer rf = Integer.parseInt(rfS);
            String forceMasterS = J6.orDefault(meta.get("forceMaster"), "false");
            boolean forceMaster = Boolean.parseBoolean(forceMasterS);
            if (forceMaster) { // they better all have the same rfactor or weird things are going to happen^^
                masterRepSize = rf;
            }
            sizes.add(rf);
            if (UnsignedInts.remainder(rf, 2) == 0) {
                BootstrapServer.LOG.warn("Schema {} uses a replication factor of {}. "
                        + "It is recommended to use uneven factors.",
                        schemas.schemaNames.get(id), rf);
            }
            if (rf < 3) {
                throw new RuntimeException("Replication factor for any schema must be >=3!");
            }
            if (rf > hosts.size()) {
                throw new RuntimeException("Replication factor for any schema can't be larger than the initial number of hosts!"
                        + "If you think you have enough hosts for rf=" + rf + " consider incrementing the value of caraca.bootThreshold.");
            }

        }
        if (masterRepSize < 0) { // in case there are no forceMaster schemata
            masterRepSize = 3;
        }
        return sizes;
    }

    public ArrayList<Integer[]> generateReplicationSetsOfSize(Set<Address> hosts, int rfactor) {
        /*
         For an explanation of the algorithm used here see:
         "Copysets: Reducing the Frequency of Data Loss in Cloud Storage"
         */
        int numberOfPermutations = (int) Math.ceil((double) scatterWidth / (double) (rfactor - 1));
        if (numberOfPermutations < 1) {
            numberOfPermutations = 1;
            System.out.println("WARNING: The Number of Permutations should not be below 1! Something is weird with your scatterWidth!");
        }
        System.out.println("INFO: Using " + numberOfPermutations + " permutations to generate initial replication sets.");
        List<Integer> nats = naturals(hosts.size());
        HashSet<TreeSet<Integer>> copysets = new HashSet<TreeSet<Integer>>();
        int p = 0;
        while (p < numberOfPermutations) {
            List<Integer> perm = new ArrayList<Integer>(nats);
            Collections.shuffle(perm, RAND);
            // split permutation into sets of size rfactor
            List<TreeSet<Integer>> permSets = new ArrayList<TreeSet<Integer>>(perm.size() / rfactor);
            boolean invalidPerm = false;
            for (int i = 0; i <= perm.size() - rfactor; i += rfactor) {
                TreeSet<Integer> set = new TreeSet<Integer>();
                for (int j = 0; j < rfactor; j++) {
                    set.add(perm.get(i + j));
                }
                if (copysets.contains(set)) { // if we create duplicate sets we need to generate another permutation
                    invalidPerm = true;
                    break;
                }
                permSets.add(set);
            }
            if (invalidPerm) {
                continue; // see above
            }
            copysets.addAll(permSets);
            p++;
        }
        ArrayList<Integer[]> res = new ArrayList<Integer[]>();
        for (TreeSet<Integer> copyset : copysets) {
            Integer[] set = new Integer[copyset.size()];
            copyset.toArray(set);
            res.add(set);
        }
        return res;
        /*
         Old code below
         */
//        ArrayList<Integer> dup1, dup2, dup3;
//        List<Integer> nats = naturals(hosts.size());
//        dup1 = new ArrayList<Integer>(nats);
//        dup2 = new ArrayList<Integer>(nats);
//        dup3 = new ArrayList<Integer>(nats);
//
//        replicationSets = new ArrayList<Integer[]>(INIT_REP_FACTOR * hosts.size());
//        replicationSetVersions = new ArrayList<Integer>(INIT_REP_FACTOR * hosts.size());
//
//        for (int n = 0; n < INIT_REP_FACTOR; n++) {
//            Collections.shuffle(dup1, RAND);
//            Collections.shuffle(dup2, RAND);
//            Collections.shuffle(dup3, RAND);
//            for (int i = 0; i < hosts.size(); i++) {
//                int h1, h2, h3;
//                h1 = dup1.get(i);
//                h2 = dup2.get(i);
//                h3 = dup3.get(i);
//                while (h2 == h1) {
//                    h2 = RAND.nextInt(hosts.size());
//                }
//                while ((h3 == h1) || (h3 == h2)) {
//                    h3 = RAND.nextInt(hosts.size());
//                }
//                Integer[] group = new Integer[]{h1, h2, h3};
//                int pos = n * hosts.size() + i;
//                replicationSets.add(pos, group);
//                replicationSetVersions.add(pos, 0);
//            }
//        }
    }

    private void generateInitialVirtuals(ByteBuffer schemaId) {
        ImmutableMap<String, String> meta = schemas.metaData.get(schemaId);
        //String rfactorS = meta.getOrDefault("rfactor", "3");
        String rfactorS = J6.orDefault(meta.get("rfactor"), "3");
        Integer rfactor = Integer.parseInt(rfactorS);
        //String vnodesS = meta.getOrDefault("vnodes", "1");
        String vnodesS = J6.orDefault(meta.get("vnodes"), "1");
        long vnodes = Long.parseLong(vnodesS);
        //String forceMasterS = meta.getOrDefault("forceMaster", "false");
        String forceMasterS = J6.orDefault(meta.get("forceMaster"), "false"); // it would look so nice in Java8 -.-
        boolean forceMaster = Boolean.parseBoolean(forceMasterS);

        // boundary nodes
        Key start = new Key(schemaId);
        //Key end = start.inc();
        Integer rset = forceMaster ? 0 : findReplicationSetOfSize(rfactor);
        virtualHostsPut(start, rset);
        //virtualHostsPut(end, rset); // this mind end up being override by the next schema, but that's ok since we only need it if there is no next schema
        if (vnodes == 1) { // single vnode needed
            return;
        }
        vnodes--; // account for the initial vnode already created (the end-nodes doesn't count)
        Set<Key> subkeys = new TreeSet<Key>();
        if (vnodes <= UnsignedBytes.MAX_VALUE) { // another byte for subkeys needed
            int incr = (int) UnsignedBytes.MAX_VALUE / (int) vnodes;
            int last = 0;
            int ceiling = (int) UnsignedBytes.MAX_VALUE - incr;
            while (last < ceiling) {
                last = last + incr;
                Key k = start.append(new byte[]{UnsignedBytes.saturatedCast(last)}).get();
                subkeys.add(k);
            }
        } else if (vnodes <= UnsignedInteger.MAX_VALUE.longValue()) { // another 4 bytes for subkeys needed
            long incr = UnsignedInteger.MAX_VALUE.longValue() / vnodes;
            long last = 0;
            long ceiling = UnsignedInteger.MAX_VALUE.longValue() - incr;
            while (last < ceiling) {
                last = last + incr;
                Key k = start.append(new Key(UnsignedInteger.valueOf(last).intValue())).get();
                subkeys.add(k);
            }
        } else { // another 8 bytes for subkeys needed (don't support more!)
            UnsignedLong incr = UnsignedLong.MAX_VALUE.dividedBy(UnsignedLong.valueOf(vnodes));
            UnsignedLong last = UnsignedLong.ZERO;
            UnsignedLong ceiling = UnsignedLong.MAX_VALUE.minus(incr);
            while (last.compareTo(ceiling) <= 0) {
                last = last.plus(incr);
                Key k = start.append(new Key(last.intValue())).get();
                subkeys.add(k);
            }
        }
        for (Key subkey : subkeys) {
            virtualHostsPut(subkey, forceMaster ? 0 : findReplicationSetOfSize(rfactor));
        }
    }

    private Integer findReplicationSetOfSize(int rfactor) {
        int bound = replicationSets.size() * 3; // just trying to avoid endless loops...if it takes longer than this to find one something is probably wrong
        int it = 0;
        while (it < bound) {
            int id = RAND.nextInt(replicationSets.size());
            Integer[] rset = replicationSets.get(id);
            if ((rset != null) && (rset.length == rfactor)) {
                return id;
            }
            it++;
        }
        System.out.println("ERROR: Couldn't find a replication set of the correct size in a realistic time frame.");
        return -1; // this will end up with an index out of bounds exception...not sure if that's the best way to handle things
    }

    private void fixRepSetsToIncludeBootstrapNodeInMasterGroup(Address self) {
        int index = 0;
        int selfId = -1;
        for (Address addr : hosts) {
            if (addr.equals(self)) {
                selfId = index;
                break;
            }
            index++;
        }
        assert (selfId >= 0);

        int target = -1;
        index = 0;
        for (Integer[] rs : replicationSets) {
            if ((rs.length == masterRepSize) && (positionInSet(rs, selfId) >= 0)) {
                target = index;
                break;
            }
            index++;
        }
        if (target < 0) { // bootstrap node doesn't occur in any group of the right size (this can happen depending on the values for scatterWidth and the number of nodes)
            // find any group of the right size instead
            index = 0;
            for (Integer[] rs : replicationSets) {
                if ((rs.length == masterRepSize)) {
                    target = index;
                    break;
                }
                index++;
            }
            assert (target >= 0); // the MUST be any group of the right size
            // simply pick a node from that group and replace it
            Integer[] rs = replicationSets.get(target);
            rs[0] = selfId;
        }
        // and switch the target with pos 0
        Integer[] tmp = replicationSets.get(target);
        replicationSets.set(0, replicationSets.get(0));
        replicationSets.set(0, tmp);
    }

    Iterator<Address> hostIterator() {
        return hosts.iterator();
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

    Address findDest(Key k, Address self, Random rand) throws NoResponsibleForKeyException, NoSuchSchemaException {
        Address[] repGroup = getResponsibles(k);
        if (repGroup == null) {
            throw new NoResponsibleForKeyException(k);
        }
        // Try to deliver locally
        for (Address adr : repGroup) {
            if (adr.sameHostAs(self)) {
                return adr;
            }
        }
        // Otherwise just pick at random
        int nodePos = RAND.nextInt(repGroup.length);
        Address dest = repGroup[nodePos];
        return dest;
    }

    /*
     * Expose internal variable for updates via LUTUpdate. Don't use this for
     * anything else!
     */
    SchemaData schemas() {
        return this.schemas;
    }

    ArrayList<Address> hosts() {
        return this.hosts;
    }

    ArrayList<Integer> replicationSetVersions() {
        return this.replicationSetVersions;
    }

    ArrayList<Integer[]> replicationSets() {
        return this.replicationSets;
    }

    /**
     * @param key
     * @return <hostGroupId, <replicationGroupKey, replicationGroupId>>
     */
    private Pair<Integer, Pair<Key, Integer>> virtualHostsGetResponsibleWithGid(Key key) throws NoSuchSchemaException {
        int groupId = key.getFirstByte();
        LookupGroup keyGroup = virtualHostGroups[groupId];
        ByteBuffer schemaId = getSchemaId(key);
        while (true) {
            try {
                Pair<Key, Integer> i = keyGroup.getResponsible(key);
                if (i.getValue0().hasPrefix(schemaId)) {
                    return Pair.with(groupId, i);
                } else {
                    return Pair.with(groupId, null); // the node that should be responsible for key is not in the same schema
                }
            } catch (NoResponsibleInGroup e) {
                groupId--;
                if (groupId < 0) {
                    return null;
                }
                keyGroup = virtualHostGroups[groupId];
            }
        }
    }

    /**
     * @param key
     * @return <replicationGroupKey, replicationGroupId>
     */
    Pair<Key, Integer> virtualHostsGetResponsible(Key key) throws NoSuchSchemaException {
        Pair<Integer, Pair<Key, Integer>> result = virtualHostsGetResponsibleWithGid(key);
        return result == null ? null : result.getValue1();
    }

    Key virtualHostsGetSuccessor(Key vnodeKey) throws NoSuchSchemaException {
        int groupId = vnodeKey.getFirstByte();
        LookupGroup keyGroup = virtualHostGroups[groupId];
        ByteBuffer schemaId = getSchemaId(vnodeKey);
        while (true) {
            try {
                Key k = keyGroup.getSuccessor(vnodeKey);
                if (k.hasPrefix(schemaId)) {
                    return k;
                } else {
                    //System.out.println("Node " + k + " doesn't have prefix " + schemaId + " of node " + vnodeKey);
                    return null; // the node's at the schema's end
                }
            } catch (NoResponsibleInGroup e) {
                groupId++;
                if (groupId >= virtualHostGroups.length) {
                    //System.out.println("End of vHostGroups for key " + vnodeKey);
                    return null;
                }
                keyGroup = virtualHostGroups[groupId];
            }
        }
    }

    Map<Address, Integer> getIdsForAddresses(ImmutableSet<Address> addresses) {
        TreeSet<Address> remaining = new TreeSet<Address>(addresses);
        TreeMap<Address, Integer> m = new TreeMap<Address, Integer>();
        int index = 0;
        for (Address addr : hosts) {
            if (remaining.isEmpty()) {
                return m;
            }
            if (remaining.remove(addr)) {
                m.put(addr, index);
            }
            index++;
        }
        return m;
    }

    public ByteBuffer getSchemaId(Key k) throws NoSuchSchemaException {
        for (ByteBuffer schemaId : schemas.schemaNames.keySet()) {
            if (k.hasPrefix(schemaId)) {
                return schemaId;
            }
        }
        throw new NoSuchSchemaException(k);
    }

    private static List<Integer> naturals(int upTo) {
        ArrayList<Integer> nats = new ArrayList<Integer>(upTo);
        for (int i = 0; i < upTo; i++) {
            nats.add(i);
        }
        return nats;
    }

    public Integer[] getReplicationGroup(Key key) {
        int groupId = key.getFirstByte();
        LookupGroup keyGroup = virtualHostGroups[groupId];
        Integer rgId = keyGroup.get(key);
        if (rgId == null) {
            return null;
        }
        return replicationSets.get(rgId);
    }

    public static class NoResponsibleInGroup extends Throwable {

        public static final NoResponsibleInGroup exception = new NoResponsibleInGroup();
    }

    public static class BrokenLut extends Throwable {

        public static final BrokenLut exception = new BrokenLut();
    }

    public static int positionInSet(Integer[] set, Integer id) {
        int index = 0;
        for (int i : set) {
            if (i == id) {
                return index;
            }
            index++;
        }
        return -1;
    }

    public static class NoResponsibleForKeyException extends Exception {

        public final Key key;

        public NoResponsibleForKeyException(Key k) {
            key = k;
        }

        @Override
        public String getMessage() {
            return "No Node found reponsible for key " + key;
        }

    }

    public static class NoSuchSchemaException extends Exception {

        public final Key key;

        public NoSuchSchemaException(Key k) {
            this.key = k;
        }

        @Override
        public String getMessage() {
            return "No Schema found that contains key " + key;
        }
    }
}
