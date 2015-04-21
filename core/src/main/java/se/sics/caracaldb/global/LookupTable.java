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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import com.larskroll.common.ByteArrayFormatter;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.AddressSerializer;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;

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

    LookupTable() {
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
            ByteArrayFormatter.printFormat(id, sb);
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
            AddressSerializer.INSTANCE.toBinary(addr, buf);
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

    public synchronized static LookupTable deserialise(byte[] bytes) throws IOException {

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
            Address addr = (Address) AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
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

    Integer findReplicationSetOfSize(int rfactor) {
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

    void setMasterRepSize(int size) {
        this.masterRepSize = size;
    }

    int getMasterRepSize() {
        return this.masterRepSize;
    }

    void setSchemas(SchemaData schemas) {
        this.schemas = schemas;
    }

    void setHosts(Collection<Address> hosts) {
        this.hosts = new ArrayList<Address>(hosts);
    }

    void setScatterWidth(int scatterWidth) {
        this.scatterWidth = scatterWidth;
    }

    ArrayList<Integer[]> resetRepSets() {
        replicationSets = new ArrayList<Integer[]>();
        return replicationSets;
    }

    void resetRepSetVersions(int version) {
        replicationSetVersions = new ArrayList<Integer>(replicationSets.size());
        for (int i = 0; i < replicationSets.size(); i++) {
            replicationSetVersions.add(i, version);
        }
    }

    public ByteBuffer getSchemaId(Key k) throws NoSuchSchemaException {
        for (ByteBuffer schemaId : schemas.schemaNames.keySet()) {
            if (k.hasPrefix(schemaId)) {
                return schemaId;
            }
        }
        throw new NoSuchSchemaException(k);
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
    
    public String asJson() {
        return LUTJsonProtocol.getLUT(versionId, schemas, hosts, replicationSets, replicationSetVersions, virtualHostGroups, virtualHostGroupVersions);
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
