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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.collect.SortedMapDifference;
import com.google.common.collect.TreeMultimap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeSet;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.global.LUTUpdate.Action;
import se.sics.caracaldb.global.LUTUpdate.PutHost;
import se.sics.caracaldb.global.LUTUpdate.PutReplicationSet;
import se.sics.caracaldb.system.Stats;
import se.sics.caracaldb.utils.ExtremeKMap;
import se.sics.kompics.address.Address;

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

    private LookupTable lut;
    private double memoryAvg = 0.0;
    private double cpuAvg = 0.0;
    private long averageHostSize = 0;

    @Override
    public void init(LookupTable lut) {
        this.lut = lut;
    }

    @Override
    public LUTUpdate rebalance(ImmutableSet<Address> joins, ImmutableSet<Address> fails, ImmutableMap<Address, Stats.Report> stats) {
        Pair<ExtremeKMap<Double, Address>, ExtremeKMap<Double, Address>> xKs = updateAverages(stats);
        ExtremeKMap<Double, Address> xKMemory = xKs.getValue0();
        ExtremeKMap<Double, Address> xKCpu = xKs.getValue1();

        double upperCpuLimit = cpuAvg + cpuAvg * THRESHOLD;
        double lowerCpuLimit = cpuAvg - cpuAvg * THRESHOLD;
        double upperMemoryLimit = memoryAvg + memoryAvg * THRESHOLD;
        double lowerMemoryLimit = memoryAvg - memoryAvg * THRESHOLD;

        ImmutableSortedMap<Integer, Address> failIds = getIdsForFails(fails);
        ImmutableSortedMap<Integer, Address> joinIds = getIdsForJoins(joins, failIds);

        ArrayList<Action> hostActions = getHostActions(failIds, joinIds);

        ArrayList<Action> relocActions = replaceFailedNodes(failIds, joinIds, xKMemory, xKCpu, stats);

        if (relocActions.size() > MAX_ACTIONS) {
            // Don't try any more balancing...there'll already be enough data movement
            return assembleUpdate(ImmutableList.of((List<Action>) hostActions, (List<Action>) relocActions));
        }

        // VERY conservative balacer
        ArrayList<Action> rebalanceActions = new ArrayList<Action>();
        // TODO make better^^
        Entry<Double, Address> topMemory = xKMemory.top().ceilingEntry();
        Entry<Double, Address> bottomMemory = xKMemory.bottom().ceilingEntry();
        if ((topMemory.getKey() > upperMemoryLimit) && (bottomMemory.getKey() < lowerMemoryLimit)) {
            switchSizeBalance(topMemory.getValue(), bottomMemory.getValue(), rebalanceActions, stats);
        }
        if (relocActions.size() + rebalanceActions.size() > MAX_ACTIONS) {
            // Don't try any more balancing...there'll already be enough data movement
            return assembleUpdate(ImmutableList.of((List<Action>) hostActions, (List<Action>) relocActions, (List<Action>) rebalanceActions));
        }
        Entry<Double, Address> topCpu = xKCpu.top().ceilingEntry();
        Entry<Double, Address> bottomCpu = xKCpu.bottom().ceilingEntry();
        if ((topCpu.getKey() > upperCpuLimit) && (bottomCpu.getKey() < lowerCpuLimit)) {
            switchOperationBalance(topCpu.getValue(), bottomCpu.getValue(), rebalanceActions, stats);
        }

        return assembleUpdate(ImmutableList.of((List<Action>) hostActions, (List<Action>) relocActions, (List<Action>) rebalanceActions));
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
        LOG.info("{}: Current cluster stats: Memory: {}%, CPU: {}% -- Moving: Memory: {}%, CPU: {}%", newMemAvg, newCpuAvg, memoryAvg, cpuAvg);
        return Pair.with(xKMemory, xKCpu);
    }

    private ImmutableSortedMap<Integer, Address> getIdsForJoins(ImmutableSet<Address> joins, ImmutableSortedMap<Integer, Address> failIds) {
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
        ArrayList<Address> hosts = lut.hosts();
        int index = 0;
        for (Address addr : hosts) {
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

    private ImmutableSortedMap<Integer, Address> getIdsForFails(ImmutableSet<Address> fails) {
        Set<Address> remaining = new HashSet<Address>(fails);
        ImmutableSortedMap.Builder<Integer, Address> ids = ImmutableSortedMap.naturalOrder();
        if (remaining.isEmpty()) {
            return ids.build();
        }
        ArrayList<Address> hosts = lut.hosts();
        int index = 0;
        for (Address addr : hosts) {
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

    private ArrayList<Action> getHostActions(ImmutableSortedMap<Integer, Address> failIds, ImmutableSortedMap<Integer, Address> joinIds) {
        SortedMapDifference<Integer, Address> idDiff = Maps.difference(failIds, joinIds);
        SortedMap<Integer, Address> nullableIds = idDiff.entriesOnlyOnLeft();
        ArrayList<Action> actions = new ArrayList<Action>();
        for (Entry<Integer, Address> e : nullableIds.entrySet()) {
            actions.add(new PutHost(e.getKey(), null));
        }
        for (Entry<Integer, Address> e : joinIds.entrySet()) {
            actions.add(new PutHost(e.getKey(), e.getValue()));
        }
        return actions;
    }

    private ArrayList<Action> replaceFailedNodes(ImmutableSortedMap<Integer, Address> failIds,
            ImmutableSortedMap<Integer, Address> joinIds, ExtremeKMap<Double, Address> xKMemory,
            ExtremeKMap<Double, Address> xKCpu, ImmutableMap<Address, Stats.Report> stats) {

        TreeSet<Integer> idsToDealWith = new TreeSet<Integer>(failIds.keySet());
        TreeMultimap<Long, Integer> candidates = TreeMultimap.create();
        // If a node fails and rejoins immediately, assign the same id and don't touch 
        // its replicationSets, since it may still have data from before the failure
        for (Entry<Integer, Address> e : joinIds.entrySet()) {
            Address curNode = lut.getHost(e.getKey());
            if (curNode.equals(e.getValue())) {
                idsToDealWith.remove(e.getKey());
                candidates.put(0l, e.getKey());
            }
        }
        // Add nodes with lowest resource usage to candidates
        ImmutableSet.Builder<Address> candidateAddrs = ImmutableSet.builder(); // still need to look up their ids
        candidateAddrs.addAll(xKMemory.bottom().values())
                .addAll(xKCpu.bottom().values()).build();
        Map<Address, Integer> candidateIds = lut.getIdsForAddresses(candidateAddrs.build());
        for (Entry<Address, Integer> e : candidateIds.entrySet()) {
            Address addr = e.getKey();
            Integer id = e.getValue();
            Stats.Report rep = stats.get(addr);
            long curSize = rep.averageSize * rep.numberOfVNodes;
            candidates.put(curSize, id);
        }

        ArrayList<Action> actions = new ArrayList<Action>();
        // Replace nodes in affected sets
        int index = 0;
        for (Integer[] member : lut.replicationSets()) {
            Integer[] newRepSet = Arrays.copyOf(member, member.length);
            for (int pos = 0; pos < member.length; pos++) {
                if (idsToDealWith.contains(member[pos])) {
                    long lowestSize = candidates.keySet().first();
                    if (lowestSize > averageHostSize) {
                        addMoreCandidates(candidates, stats);
                    }
                    // pick the first (lowestSize) host not in the replicationSet
                    long curSize = -1;
                    long addedSize = -1;
                    for (Entry<Long, Integer> e : candidates.entries()) {
                        if (LookupTable.positionInSet(newRepSet, e.getValue()) < 0) {
                            newRepSet[pos] = e.getValue();
                            curSize = e.getKey();
                            addedSize = guessAddedSize(member, stats);
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
                actions.add(new PutReplicationSet(index, lut.replicationSetVersions().get(index) + 1, newRepSet));
            }
            index++;
        }
        return actions;
    }

    private void addMoreCandidates(TreeMultimap<Long, Integer> candidates, ImmutableMap<Address, Stats.Report> stats) {
        if (candidates.values().size() >= stats.size()) {
            return; // Already everyone a candidate...nothing to do
        }
        int index = 0;
        for (Address addr : lut.hosts()) {
            if (!candidates.containsValue(index)) {
                Stats.Report rep = stats.get(index);
                if (rep != null) {
                    long curSize = rep.averageSize * rep.numberOfVNodes;
                    candidates.put(curSize, index);
                }
            }
            index++;
        }
    }

    private long guessAddedSize(Integer[] member, ImmutableMap<Address, Stats.Report> stats) {
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

    private LUTUpdate assembleUpdate(ImmutableList<List<Action>> actions) {
        int totalActions = 0;
        for (List<Action> actionsL : actions) {
            totalActions += actionsL.size();
        }
        if (totalActions == 0) {
            return null; // nothing to do, no reason to update
        }
        Action[] acts = new Action[totalActions];
        int index = 0;
        for (List<Action> actionsL : actions) {
            for (Action a : actionsL) {
                acts[index] = a;
                index++;
            }
        }
        return new LUTUpdate(lut.versionId, lut.versionId + 1, acts);
    }

    private void switchSizeBalance(Address topAddr, Address bottomAddr, ArrayList<Action> rebalanceActions, ImmutableMap<Address, Stats.Report> stats) {
        Stats.Report topRep = stats.get(topAddr);
        Stats.Report bottomRep = stats.get(bottomAddr);
        Map<Address, Integer> ids = lut.getIdsForAddresses(ImmutableSet.of(topAddr, bottomAddr));
        for (Key k : topRep.topKSize) {
            Integer repGroupId = lut.virtualHostsGet(k);
            if (repGroupId == null) {
                continue;
            }
            Integer[] repGroup = lut.replicationSets().get(repGroupId);
            int topPos = LookupTable.positionInSet(repGroup, ids.get(topAddr));
            int bottomPos = LookupTable.positionInSet(repGroup, ids.get(bottomAddr));
            if (bottomPos < 0) { // new address is not already part of the replication group
                Integer[] newRepGroup = Arrays.copyOf(repGroup, repGroup.length);
                newRepGroup[topPos] = ids.get(bottomAddr);
                lut.findGroupOrAddNew(k, newRepGroup, rebalanceActions);
                return;
            }
        }
        // if all of the topKSize vNodes already share a group with the bottomAddr there's nothing we can do
    }

    private void switchOperationBalance(Address topAddr, Address bottomAddr, ArrayList<Action> rebalanceActions, ImmutableMap<Address, Stats.Report> stats) {
        Stats.Report topRep = stats.get(topAddr);
        //Stats.Report bottomRep = stats.get(bottomAddr);
        Map<Address, Integer> ids = lut.getIdsForAddresses(ImmutableSet.of(topAddr, bottomAddr));
        for (Key k : topRep.topKOps) {
            Integer repGroupId = lut.virtualHostsGet(k);
            if (repGroupId == null) {
                continue;
            }
            Integer[] repGroup = lut.replicationSets().get(repGroupId);
            int topPos = LookupTable.positionInSet(repGroup, ids.get(topAddr));
            int bottomPos = LookupTable.positionInSet(repGroup, ids.get(bottomAddr));
            if (bottomPos < 0) { // new address is not already part of the replication group
                Integer[] newRepGroup = Arrays.copyOf(repGroup, repGroup.length);
                newRepGroup[topPos] = ids.get(bottomAddr);
                lut.findGroupOrAddNew(k, newRepGroup, rebalanceActions);
                return;
            }
        }
        // if all of the topKOps vNodes already share a group with the bottomAddr there's nothing we can do
    }

}
