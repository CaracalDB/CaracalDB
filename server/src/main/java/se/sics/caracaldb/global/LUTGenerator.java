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
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedInts;
import com.google.common.primitives.UnsignedLong;
import com.larskroll.common.J6;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.bootstrap.BootstrapServer;
import static se.sics.caracaldb.global.LookupTable.RAND;
import se.sics.caracaldb.system.Configuration;

/**
 *
 * @author lkroll
 */
public class LUTGenerator {

    public static LookupTable generateInitial(Set<Address> hosts, Configuration config, Address self) {
        LookupTable lut = new LookupTable();
        lut.setScatterWidth(config.getInt("caracal.scatterWidth"));
        generateHosts(lut, hosts);
        loadSchemas(lut, config);
        // Generate initial replication sets for all requested sizes
        Set<Integer> rfSizes = findReplicationSetSizes(lut);
        ArrayList<Integer[]> repSets = lut.resetRepSets();
        for (Integer rf : rfSizes) {
            ArrayList<Integer[]> rss = generateReplicationSetsOfSize(lut, hosts, rf);
            repSets.addAll(rss);
        }
        fixRepSetsToIncludeBootstrapNodeInMasterGroup(lut, self); // This is optional but a simple optimisation
        // Set all versions to 0
        lut.resetRepSetVersions(0);
        for (ByteBuffer schemaId : lut.schemas().metaData.keySet()) {
            generateInitialVirtuals(lut, schemaId);
        }

        return lut;
    }

    private static void generateHosts(LookupTable lut, Set<Address> hosts) {
        lut.setHosts(hosts);
    }

    private static void loadSchemas(LookupTable lut, Configuration config) {
        lut.setSchemas(SchemaReader.importSchemas(config));
    }

    private static Set<Integer> findReplicationSetSizes(LookupTable lut) {
        Set<Integer> sizes = new TreeSet<Integer>();
        for (Map.Entry<ByteBuffer, ImmutableMap<String, String>> e : lut.schemas().metaData.entrySet()) {
            ByteBuffer id = e.getKey();
            ImmutableMap<String, String> meta = e.getValue();
            String rfS = J6.orDefault(meta.get("rfactor"), "3");
            Integer rf = Integer.parseInt(rfS);
            String forceMasterS = J6.orDefault(meta.get("forceMaster"), "false");
            boolean forceMaster = Boolean.parseBoolean(forceMasterS);
            if (forceMaster) { // they better all have the same rfactor or weird things are going to happen^^
                lut.setMasterRepSize(rf);
            }
            sizes.add(rf);
            if (UnsignedInts.remainder(rf, 2) == 0) {
                BootstrapServer.LOG.warn("Schema {} uses a replication factor of {}. "
                        + "It is recommended to use uneven factors.",
                        lut.schemas().schemaNames.get(id), rf);
            }
            if (rf < 3) {
                throw new RuntimeException("Replication factor for any schema must be >=3!");
            }
            if (rf > lut.hosts().size()) {
                throw new RuntimeException("Replication factor for any schema can't be larger than the initial number of hosts!"
                        + "If you think you have enough hosts for rf=" + rf + " consider incrementing the value of caraca.bootThreshold.");
            }

        }
        if (lut.getMasterRepSize() < 0) { // in case there are no forceMaster schemata
            lut.setMasterRepSize(3);
        }
        return sizes;
    }

    private static ArrayList<Integer[]> generateReplicationSetsOfSize(LookupTable lut, Set<Address> hosts, int rfactor) {
        /*
         For an explanation of the algorithm used here see:
         "Copysets: Reducing the Frequency of Data Loss in Cloud Storage"
         */
        int numberOfPermutations = (int) Math.ceil((double) lut.getScatterWidth() / (double) (rfactor - 1));
        if (numberOfPermutations < 1) {
            numberOfPermutations = 1;
            System.out.println("WARNING: The Number of Permutations should not be below 1! Something is weird with your scatterWidth!");
        }
        System.out.println("INFO: Using " + numberOfPermutations + " permutations to generate initial replication sets.");
        List<Integer> nats = naturals(hosts.size());
        Set<Set<Integer>> copysets = new HashSet<Set<Integer>>();
        int p = 0;
        while (p < numberOfPermutations) {
            List<Integer> perm = new ArrayList<Integer>(nats);
            Collections.shuffle(perm, RAND);
            // split permutation into sets of size rfactor
            List<Set<Integer>> permSets = new ArrayList<Set<Integer>>(perm.size() / rfactor);
            boolean invalidPerm = false;
            for (int i = 0; i <= perm.size() - rfactor; i += rfactor) {
                Set<Integer> set = new HashSet<Integer>();
                for (int j = 0; j < rfactor; j++) {
                    set.add(perm.get(i + j));
                }
                if (copysets.contains(set)) { // if we create duplicate sets we need to generate another permutation
                    //System.out.println("Duplicate set " + set + " detected by HashSet.contains!");
                    invalidPerm = true;
                    break;
                }
// The code below was for testing if set.contains works properly...apparantly it does...leaving it here for reference
//                for (Set<Integer> existing : copysets) {
//                    if (existing.equals(set)) {
//                        System.out.println("Duplicate set " + set + " - " + existing + "was not detected by HashSet.contains!");
//                        invalidPerm = true;
//                        break;
//                    }
//                    boolean same = true;
//                    for (Integer val : existing) {
//                        if (!set.contains(val)) {
//                            same = false;
//                        }
//                    }
//                    if (same) {
//                        System.out.println("Duplicate set " + set + " - " + existing + "was not detected by TreeSet.equals!");
//                        invalidPerm = true;
//                        break;
//                    }
//                }
                permSets.add(set);
            }
            if (invalidPerm) {
                continue; // see above
            }
            copysets.addAll(permSets);
            p++;
        }
        ArrayList<Integer[]> res = new ArrayList<Integer[]>(copysets.size());
        for (Set<Integer> copyset : copysets) {
            Integer[] set = new Integer[copyset.size()];
            copyset.toArray(set);
            Arrays.sort(set);
            //System.out.println(copyset+" -> " + Arrays.toString(set));
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

    private static void generateInitialVirtuals(LookupTable lut, ByteBuffer schemaId) {
        ImmutableMap<String, String> meta = lut.schemas().metaData.get(schemaId);
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
        Integer rset = forceMaster ? 0 : lut.findReplicationSetOfSize(rfactor);
        lut.virtualHostsPut(start, rset);
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
            lut.virtualHostsPut(subkey, forceMaster ? 0 : lut.findReplicationSetOfSize(rfactor));
        }
    }

    private static void fixRepSetsToIncludeBootstrapNodeInMasterGroup(LookupTable lut, Address self) {
        int index = 0;
        Integer selfId = -1;
        for (Address addr : lut.hosts()) {
            if (addr.equals(self)) {
                selfId = index;
                break;
            }
            index++;
        }
        assert (selfId >= 0);

        int target = -1;
        index = 0;
        for (Integer[] rs : lut.replicationSets()) {
            if ((rs.length == lut.getMasterRepSize()) && (LookupTable.positionInSet(rs, selfId) >= 0)) {
                target = index;
                break;
            }
            index++;
        }
        if (target < 0) { // bootstrap node doesn't occur in any group of the right size (this can happen depending on the values for scatterWidth and the number of nodes)
            // find any group of the right size instead
            index = 0;
            for (Integer[] rs : lut.replicationSets()) {
                if ((rs.length == lut.getMasterRepSize())) {
                    target = index;
                    break;
                }
                index++;
            }
            assert (target >= 0); // the MUST be any group of the right size
            // simply pick a node from that group and replace it
            Integer[] rs = lut.replicationSets().get(target);
            rs[0] = selfId;
        }
        // and switch the target with pos 0
        Integer[] tmp = lut.replicationSets().get(target);
        lut.replicationSets().set(target, lut.replicationSets().get(0));
        lut.replicationSets().set(0, tmp);
    }

    private static List<Integer> naturals(int upTo) {
        ArrayList<Integer> nats = new ArrayList<Integer>(upTo);
        for (int i = 0; i < upTo; i++) {
            nats.add(i);
        }
        return nats;
    }
}
