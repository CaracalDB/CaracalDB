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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.SortedMapDifference;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import com.larskroll.common.ExtremeKMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.system.Stats;
import se.sics.caracaldb.utils.HashIdGenerator;

/**
 *
 * @author lkroll
 */
public class DefaultPolicy implements MaintenancePolicy {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultPolicy.class);

    private static final double ALPHA = 0.16;
    private static final double MINUS_ALPHA = 1 - ALPHA;
    private static final int K = 5;
    private static final double THRESHOLD = 0.35;
    private static final int MAX_ACTIONS = 5;

    private HashIdGenerator idGen;
    private double memoryAvg = 0.0;
    private double cpuAvg = 0.0;
    private long averageHostSize = 0;

    @Override
    public void init(HashIdGenerator idGen) {
        this.idGen = idGen;
    }

    @Override
    public void rebalance(LUTWorkingBuffer lut, ImmutableSet<Address> joins, ImmutableSet<Address> fails, ImmutableMap<Address, Stats.Report> stats, ImmutableSet<Schema.Req> schemaChanges) {
        Pair<ExtremeKMap<Double, Address>, ExtremeKMap<Double, Address>> xKs = updateAverages(stats);
        ExtremeKMap<Double, Address> xKMemory = xKs.getValue0();
        ExtremeKMap<Double, Address> xKCpu = xKs.getValue1();

        double upperCpuLimit = cpuAvg + cpuAvg * THRESHOLD;
        double lowerCpuLimit = cpuAvg - cpuAvg * THRESHOLD;
        double upperMemoryLimit = memoryAvg + memoryAvg * THRESHOLD;
        double lowerMemoryLimit = memoryAvg - memoryAvg * THRESHOLD;

        ImmutableSortedMap<Integer, Address> failIds = getIdsForFails(lut, fails);
        ImmutableSortedMap<Integer, Address> joinIds = getIdsForJoins(lut, joins, failIds);

        getHostActions(lut, failIds, joinIds);

        replaceFailedNodes(lut, failIds, joinIds, xKMemory, xKCpu, stats);

        getSchemaActions(lut, schemaChanges);

        if (lut.numberOfActions() > MAX_ACTIONS) {
            // Don't try any more balancing...there'll already be enough data movement
            return;
        }

	// TODO don't load balance for now...this needs to be tested
	return;
	/*
        // VERY conservative balacer
        // TODO make better^^
        Entry<Double, Address> topMemory = xKMemory.top().ceilingEntry();
        Entry<Double, Address> bottomMemory = xKMemory.bottom().ceilingEntry();
        if ((topMemory.getKey() > upperMemoryLimit) && (bottomMemory.getKey() < lowerMemoryLimit)) {
            switchSizeBalance(lut, topMemory.getValue(), bottomMemory.getValue(), stats);
        }
        if (lut.numberOfActions() > MAX_ACTIONS) {
            // Don't try any more balancing...there'll already be enough data movement
            return;
        }
        Entry<Double, Address> topCpu = xKCpu.top().ceilingEntry();
        Entry<Double, Address> bottomCpu = xKCpu.bottom().ceilingEntry();
        if ((topCpu.getKey() > upperCpuLimit) && (bottomCpu.getKey() < lowerCpuLimit)) {
            switchOperationBalance(lut, topCpu.getValue(), bottomCpu.getValue(), stats);
        }

        return;
	*/
    }

    private Pair<ExtremeKMap<Double, Address>, ExtremeKMap<Double, Address>> updateAverages(ImmutableMap<Address, Stats.Report> stats) {
        ExtremeKMap<Double, Address> xKMemory = new ExtremeKMap<Double, Address>(K);
        ExtremeKMap<Double, Address> xKCpu = new ExtremeKMap<Double, Address>(K);

        double totalCpu = 0.0;
        double totalMemory = 0.0;
        long totalClusterSize = 0;
        for (Stats.Report report : stats.values()) {
            totalCpu += report.cpuUsage;
            totalMemory += report.memoryUsage;
            totalClusterSize += report.averageSize * report.numberOfVNodes;
            xKMemory.put(report.memoryUsage, report.atHost);
            xKCpu.put(report.cpuUsage, report.atHost);
        }
        double newMemAvg = totalMemory / ((double) stats.size());
        double newCpuAvg = totalCpu / ((double) stats.size());
        averageHostSize = Stats.floorDiv(totalClusterSize, stats.size());
        // Exponential moving average with coefficient ALPHA
        memoryAvg = ALPHA * newMemAvg + MINUS_ALPHA * memoryAvg;
        cpuAvg = ALPHA * newCpuAvg + MINUS_ALPHA * cpuAvg;
        LOG.info("Current cluster stats: Memory: {}%, CPU: {}% -- Moving: Memory: {}%, CPU: {}%", newMemAvg, newCpuAvg, memoryAvg, cpuAvg);
        return Pair.with(xKMemory, xKCpu);
    }

    private ImmutableSortedMap<Integer, Address> getIdsForJoins(LUTWorkingBuffer lut, ImmutableSet<Address> joins, ImmutableSortedMap<Integer, Address> failIds) {
        TreeSet<Address> remaining = new TreeSet<Address>(joins);
        ImmutableSortedMap.Builder<Integer, Address> ids = ImmutableSortedMap.naturalOrder();
        TreeSet<Integer> usedIds = new TreeSet<Integer>();
        // If nodes fail and rejoind try to assign the same id
        for (Entry<Integer, Address> e : failIds.entrySet()) {
            if (remaining.isEmpty()) {
                return ids.build();
            }
            if (remaining.contains(e.getValue())) {
                ids.put(e);
                remaining.remove(e.getValue());
                usedIds.add(e.getKey());
            }
        }
        // Assign all the other failed ids to new nodes
        for (Entry<Integer, Address> e : failIds.entrySet()) {
            if (remaining.isEmpty()) {
                return ids.build();
            }
            if (!usedIds.contains(e.getKey())) {
                Address j = remaining.pollFirst();
                ids.put(e.getKey(), j);
                usedIds.add(e.getKey());
            }
        }
        // Look for other empty slots in the LUT
        int index = 0;
        for (Address addr : lut.hosts()) {
            if (remaining.isEmpty()) {
                return ids.build();
            }
            if (addr == null) {
                if (usedIds.contains(index)) {
                    LOG.warn("The node at index {} apparently failed twice. This is weird -.-");
                    index++;
                    continue;
                }
                Address j = remaining.pollFirst();
                ids.put(index, j);
                usedIds.add(index);
            }
            index++;
        }
        return ids.build();
    }

    private ImmutableSortedMap<Integer, Address> getIdsForFails(LUTWorkingBuffer lut, ImmutableSet<Address> fails) {
        Set<Address> remaining = new HashSet<Address>(fails);
        ImmutableSortedMap.Builder<Integer, Address> ids = ImmutableSortedMap.naturalOrder();
        if (remaining.isEmpty()) {
            return ids.build();
        }
        int index = 0;
        for (Address addr : lut.hosts()) {
            if (remaining.remove(addr)) {
                ids.put(index, addr);
                if (remaining.isEmpty()) {
                    break;
                }
            }
            index++;
        }
        return ids.build();
    }

    private void getHostActions(LUTWorkingBuffer lut, ImmutableSortedMap<Integer, Address> failIds, ImmutableSortedMap<Integer, Address> joinIds) {
        SortedMapDifference<Integer, Address> idDiff = Maps.difference(failIds, joinIds);
        SortedMap<Integer, Address> nullableIds = idDiff.entriesOnlyOnLeft();
        for (Entry<Integer, Address> e : nullableIds.entrySet()) {
            lut.removeHost(e.getKey());
        }
        for (Entry<Integer, Address> e : joinIds.entrySet()) {
            lut.putHost(e.getKey(), e.getValue());
        }
    }

    private void replaceFailedNodes(LUTWorkingBuffer lut, ImmutableSortedMap<Integer, Address> failIds,
            ImmutableSortedMap<Integer, Address> joinIds, ExtremeKMap<Double, Address> xKMemory,
            ExtremeKMap<Double, Address> xKCpu, ImmutableMap<Address, Stats.Report> stats) {

        TreeSet<Integer> idsToDealWith = new TreeSet<Integer>(failIds.keySet());
        TreeMultimap<Long, Integer> candidates = TreeMultimap.create();
        // If a node fails and rejoins immediately, assign the same id and don't touch 
        // its replicationSets, since it may still have data from before the failure
        for (Entry<Integer, Address> e : joinIds.entrySet()) {
            Address curNode = lut.lut.getHost(e.getKey());
            if (curNode.equals(e.getValue())) {
                idsToDealWith.remove(e.getKey());
                candidates.put(0l, e.getKey());
            }
        }
        // Add nodes with lowest resource usage to candidates
        ImmutableSet.Builder<Address> candidateAddrs = ImmutableSet.builder(); // still need to look up their ids
        candidateAddrs.addAll(xKMemory.bottom().values())
                .addAll(xKCpu.bottom().values()).build();
        Map<Address, Integer> candidateIds = lut.lut.getIdsForAddresses(candidateAddrs.build());
        for (Entry<Address, Integer> e : candidateIds.entrySet()) {
            Address addr = e.getKey();
            Integer id = e.getValue();
            Stats.Report rep = stats.get(addr);
            long curSize = rep.averageSize * rep.numberOfVNodes;
            candidates.put(curSize, id);
        }
        // Replace nodes in affected sets
        int index = 0;
        for (Integer[] member : lut.replicationSets()) {
            Integer[] newRepSet = Arrays.copyOf(member, member.length);
            for (int pos = 0; pos < member.length; pos++) {
                if (idsToDealWith.contains(member[pos])) {
                    long lowestSize = candidates.keySet().first();
                    if (lowestSize > averageHostSize) {
                        addMoreCandidates(lut, candidates, stats);
                    }
                    // pick the first (lowestSize) host not in the replicationSet
                    long curSize = -1;
                    long addedSize = -1;
                    for (Entry<Long, Integer> e : candidates.entries()) {
                        if (LookupTable.positionInSet(newRepSet, e.getValue()) < 0) {
                            newRepSet[pos] = e.getValue();
                            curSize = e.getKey();
                            addedSize = guessAddedSize(lut, member, stats);
                            break;
                        }
                    }
                    if ((curSize < 0) || (addedSize < 0)) {
                        LOG.error("Could not find any candidate for replacing {} in replicationSet {}!", member[pos], index);
                        continue;
                    }
                    // Update candidates
                    candidates.remove(curSize, newRepSet[pos]);
                    candidates.put(curSize + addedSize, newRepSet[pos]);
                }
            }
            if (!Arrays.equals(member, newRepSet)) {
                lut.putRepSet(index, newRepSet);
            }
            index++;
        }
    }

    private void addMoreCandidates(LUTWorkingBuffer lut, TreeMultimap<Long, Integer> candidates, ImmutableMap<Address, Stats.Report> stats) {
        if (candidates.values().size() >= stats.size()) {
            return; // Already everyone a candidate...nothing to do
        }
        int index = 0;
        for (Address addr : lut.hosts()) {
            if (!candidates.containsValue(index)) {
                Stats.Report rep = stats.get(addr);
                if (rep != null) {
                    long curSize = rep.averageSize * rep.numberOfVNodes;
                    candidates.put(curSize, index);
                }
            }
            index++;
        }
    }

    private long guessAddedSize(LUTWorkingBuffer lut, Integer[] member, ImmutableMap<Address, Stats.Report> stats) {
        // Use the average of the average node size of all live members to guess the size for this group
        ArrayList<Long> avgSizes = new ArrayList<Long>(member.length);
        for (Integer id : member) {
            Address addr = lut.getHost(id);
            Stats.Report rep = stats.get(addr);
            if (rep != null) {
                avgSizes.add(rep.averageSize);
            }
        }
        long sum = 0;
        for (Long l : avgSizes) {
            sum += l;
        }
        return Stats.floorDiv(sum, avgSizes.size());
    }

    private void switchSizeBalance(LUTWorkingBuffer lut, Address topAddr, Address bottomAddr, ImmutableMap<Address, Stats.Report> stats) {
        Stats.Report topRep = stats.get(topAddr);
        Stats.Report bottomRep = stats.get(bottomAddr);
        Map<Address, Integer> ids = lut.lut.getIdsForAddresses(ImmutableSet.of(topAddr, bottomAddr));
        for (Key k : topRep.topKSize) {
            Integer repGroupId = lut.getRepGroup(k);
            if (repGroupId == null) {
                continue;
            }
            Integer[] repGroup = lut.getRepSet(repGroupId);
            int topPos = LookupTable.positionInSet(repGroup, ids.get(topAddr));
            int bottomPos = LookupTable.positionInSet(repGroup, ids.get(bottomAddr));
            if (bottomPos < 0) { // new address is not already part of the replication group
                Integer[] newRepGroup = Arrays.copyOf(repGroup, repGroup.length);
                newRepGroup[topPos] = ids.get(bottomAddr);
                lut.findGroupOrAddNew(k, newRepGroup);
                return;
            }
        }
        // if all of the topKSize vNodes already share a group with the bottomAddr there's nothing we can do
    }

    private void switchOperationBalance(LUTWorkingBuffer lut, Address topAddr, Address bottomAddr, ImmutableMap<Address, Stats.Report> stats) {
        Stats.Report topRep = stats.get(topAddr);
        //Stats.Report bottomRep = stats.get(bottomAddr);
        Map<Address, Integer> ids = lut.lut.getIdsForAddresses(ImmutableSet.of(topAddr, bottomAddr));
        for (Key k : topRep.topKOps) {
            Integer repGroupId = lut.getRepGroup(k);
            if (repGroupId == null) {
                continue;
            }
            Integer[] repGroup = lut.getRepSet(repGroupId);
            int topPos = LookupTable.positionInSet(repGroup, ids.get(topAddr));
            int bottomPos = LookupTable.positionInSet(repGroup, ids.get(bottomAddr));
            if (bottomPos < 0) { // new address is not already part of the replication group
                Integer[] newRepGroup = Arrays.copyOf(repGroup, repGroup.length);
                newRepGroup[topPos] = ids.get(bottomAddr);
                lut.findGroupOrAddNew(k, newRepGroup);
                return;
            }
        }
        // if all of the topKOps vNodes already share a group with the bottomAddr there's nothing we can do
    }

    private void getSchemaActions(LUTWorkingBuffer lut, ImmutableSet<Schema.Req> schemaChanges) {
        for (Schema.Req req : schemaChanges) {
            if (req instanceof Schema.CreateReq) {
                Schema.CreateReq creq = (Schema.CreateReq) req;
                byte[] schemaId = idGen.idForNameDontStartWith(creq.name, LookupTable.RESERVED_PREFIX.getArray());
                SchemaData.SingleSchema schema = new SchemaData.SingleSchema(ByteBuffer.wrap(schemaId), creq.name, creq.metaData);
                lut.addSchema(schema);
                // also assign initial vnodes
                int vnodes = 1; //default
                int rfactor = 3; //default
                String vnodeS = creq.metaData.get("vnodes");
                if (vnodeS != null) {
                    vnodes = Integer.parseInt(vnodeS);
                }
                String rfactorS = creq.metaData.get("rfactor");
                if (rfactorS != null) {
                    rfactor = Integer.parseInt(rfactorS);
                }
                // find right size repsets
                ArrayList<Integer> replicationSets = new ArrayList<Integer>();
                int index = 0;
                for (Integer[] repset : lut.replicationSets()) {
                    if (repset.length == rfactor) {
                        replicationSets.add(index);
                    }
                    index++;
                }
                if (replicationSets.isEmpty()) {
                    replicationSets = lut.createRepSets(rfactor);
                }
                Set<Key> keys = generateVNodes(schemaId, vnodes);
                index = 0;
                for (Key k : keys) {
                    lut.putRepGroup(k, replicationSets.get(index));
                    index++;
                    index = index % replicationSets.size();
                }
            } else if (req instanceof Schema.DropReq) {
                Schema.DropReq dreq = (Schema.DropReq) req;
                lut.removeSchema(dreq.name);
            } else {
                LOG.error("Unkown type of schema request: {}", req);
            }
        }
    }

    private Set<Key> generateVNodes(byte[] schemaId, int num) {
        Set<Key> keys = new TreeSet<Key>();
        // boundary nodes
        Key start = new Key(schemaId);
        keys.add(start);
        //virtualHostsPut(end, rset); // this mind end up being override by the next schema, but that's ok since we only need it if there is no next schema
        if (num == 1) { // single vnode needed
            return keys;
        }
        num--; // account for the initial vnode already created (the end-nodes doesn't count)
        if (num <= UnsignedBytes.MAX_VALUE) { // another byte for subkeys needed
            int incr = (int) UnsignedBytes.MAX_VALUE / (int) num;
            int last = 0;
            int ceiling = (int) UnsignedBytes.MAX_VALUE - incr;
            while (last < ceiling) {
                last = last + incr;
                Key k = start.append(new byte[]{UnsignedBytes.saturatedCast(last)}).get();
                keys.add(k);
            }
        } else if (num <= UnsignedInteger.MAX_VALUE.longValue()) { // another 4 bytes for subkeys needed
            long incr = UnsignedInteger.MAX_VALUE.longValue() / num;
            long last = 0;
            long ceiling = UnsignedInteger.MAX_VALUE.longValue() - incr;
            while (last < ceiling) {
                last = last + incr;
                Key k = start.append(new Key(UnsignedInteger.valueOf(last).intValue())).get();
                keys.add(k);
            }
        } else { // another 8 bytes for subkeys needed (don't support more!)
            UnsignedLong incr = UnsignedLong.MAX_VALUE.dividedBy(UnsignedLong.valueOf(num));
            UnsignedLong last = UnsignedLong.ZERO;
            UnsignedLong ceiling = UnsignedLong.MAX_VALUE.minus(incr);
            while (last.compareTo(ceiling) <= 0) {
                last = last.plus(incr);
                Key k = start.append(new Key(last.intValue())).get();
                keys.add(k);
            }
        }
        return keys;
    }

}
