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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.global.LUTUpdate.Action;
import se.sics.caracaldb.global.LUTUpdate.CreateSchema;
import se.sics.caracaldb.global.LUTUpdate.DropSchema;
import se.sics.caracaldb.global.LUTUpdate.PutHost;
import se.sics.caracaldb.global.LUTUpdate.PutReplicationGroup;
import se.sics.caracaldb.global.LUTUpdate.PutReplicationSet;
import se.sics.caracaldb.global.SchemaData.SingleSchema;
import se.sics.kompics.address.Address;

/**
 *
 * @author lkroll
 */
public class LUTWorkingBuffer {

    public final LookupTable lut;

    private TreeMap<Integer, Address> hostUpdates = new TreeMap<Integer, Address>();
    private TreeMap<Integer, Integer[]> repSetUpdates = new TreeMap<Integer, Integer[]>();
    private TreeMap<Key, Integer> repGroupUpdates = new TreeMap<Key, Integer>();
    private HashMap<ByteBuffer, SingleSchema> schemaUpdates = new HashMap<ByteBuffer, SingleSchema>();

    public LUTWorkingBuffer(LookupTable lut) {
        this.lut = lut;
    }

    public int numberOfActions() {
        ArrayList<Action> actions = new ArrayList<Action>();
        getActions(actions);
        return actions.size();
    }

    public Iterable<Address> hosts() {
        return new Iterable<Address>() {

            @Override
            public Iterator<Address> iterator() {
                return new Iterator<Address>() {

                    private int index = 0;

                    @Override
                    public boolean hasNext() {
                        if (hostUpdates.isEmpty()) {
                            return lut.hosts().size() > index;
                        }
                        return (lut.hosts().size() > index) || (hostUpdates.lastKey() >= index);
                    }

                    @Override
                    public Address next() {
                        Address addr;
                        if (hostUpdates.containsKey(index)) {
                            addr = hostUpdates.get(index);
                        } else if (lut.hosts().size() > index) {
                            addr = lut.hosts().get(index);
                        } else {
                            addr = null;
                        }
                        index++;
                        return addr;
                    }
                    
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

        };
    }

    public void putHost(Integer index, Address addr) {
        hostUpdates.put(index, addr);
    }

    public void removeHost(Integer index) {
        hostUpdates.put(index, null);
    }

    public Address getHost(Integer index) {
        if (hostUpdates.containsKey(index)) {
            return hostUpdates.get(index);
        } else if (lut.hosts().size() > index) {
            return lut.hosts().get(index);
        }
        return null;
    }

    public Iterable<Integer[]> replicationSets() {
        return new Iterable<Integer[]>() {

            @Override
            public Iterator<Integer[]> iterator() {
                return new Iterator<Integer[]>() {
                    private int index = 0;

                    @Override
                    public boolean hasNext() {
                        if (repSetUpdates.isEmpty()) {
                            return lut.replicationSets().size() > index;
                        }
                        return (lut.replicationSets().size() > index) || (repSetUpdates.lastKey() >= index);
                    }

                    @Override
                    public Integer[] next() {
                        Integer[] repSet;
                        if (repSetUpdates.containsKey(index)) {
                            repSet = repSetUpdates.get(index);
                        } else if (lut.replicationSets().size() > index) {
                            repSet = lut.replicationSets().get(index);
                        } else {
                            repSet = null;
                        }
                        index++;
                        return repSet;
                    }
                    
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public void putRepSet(Integer index, Integer[] repSet) {
        repSetUpdates.put(index, repSet);
    }

    public Integer[] getRepSet(Integer index) {
        if (repSetUpdates.containsKey(index)) {
            return repSetUpdates.get(index);
        } else if (lut.replicationSets().size() > index) {
            return lut.replicationSets().get(index);
        }
        return null;
    }

    public Integer getRepGroup(Key k) {
        if (repGroupUpdates.containsKey(k)) {
            return repGroupUpdates.get(k);
        } else {
            return lut.virtualHostsGet(k);
        }
    }

    public void putRepGroup(Key k, Integer index) {
        repGroupUpdates.put(k, index);
    }

    public void findGroupOrAddNew(Key k, Integer[] newRepGroup) {
        int index = 0;
        int lastNullIndex = -1;
        for (Integer[] repGroup : replicationSets()) {
            if (repGroup == null) {
                lastNullIndex = index;
            } else if (Arrays.equals(repGroup, newRepGroup)) {
                putRepSet(index, newRepGroup);
                putRepGroup(k, index);
                return;
            }
            index++;
        }
        if (lastNullIndex >= 0) {
            putRepSet(lastNullIndex, newRepGroup);
            putRepGroup(k, lastNullIndex);
        } else {
            putRepSet(index, newRepGroup);
            putRepGroup(k, index);
        }
    }
    
    public SingleSchema getSchema(ByteBuffer id) {
        if (schemaUpdates.containsKey(id)) {
            return schemaUpdates.get(id);
        } else {
            String name = lut.schemas().schemaNames.get(id);
            if (name != null) {
                return new SingleSchema(id, name, lut.schemas().metaData.get(id));
            }
        }
        return null;
    }
    
    public void addSchema(SingleSchema schema) {
        schemaUpdates.put(schema.id, schema);
    }
    
    public void removeSchema(String name) {
        ByteBuffer id = lut.schemas().schemaIDs.get(name);
        if (id != null) {
            schemaUpdates.put(id, null);
            Set<Key> keys = lut.getVirtualNodesInSchema(new Key(id));
            for (Key k : keys) {
                repGroupUpdates.put(k, null);
            }
        }
    }

    public LUTUpdate assembleUpdate() {
        ArrayList<Action> actions = new ArrayList<Action>();
        getActions(actions);
        if (actions.isEmpty()) {
            return null; // nothing to do, no reason to update
        }
        return new LUTUpdate(lut.versionId, lut.versionId + 1, actions.toArray(new Action[0]));
    }
    
    private void getActions(List<Action> actions) {
        schemaActions(actions); // don't reorder these!
        hostActions(actions);
        repSetActions(actions);
        repGroupActions(actions);
    }
    private void hostActions(List<Action> actions) {
        if (hostUpdates.isEmpty()) {
            return;
        }
        for (int i = 0; i < hostUpdates.lastKey(); i++) {
            if (hostUpdates.containsKey(i)) {
                actions.add(new PutHost(i, hostUpdates.get(i)));
            } else if (i >= lut.hosts().size()) {
                actions.add(new PutHost(i, null)); // can't leave empty spaces
            }
        }
    }

    private void repSetActions(List<Action> actions) {
        if (repSetUpdates.isEmpty()) {
            return;
        }
        for (int i = 0; i < repSetUpdates.lastKey(); i++) {
            if (repSetUpdates.containsKey(i)) {
                int version = 0;
                if (i < lut.replicationSetVersions().size()) {
                    version = lut.replicationSetVersions().get(i) + 1;
                }
                actions.add(new PutReplicationSet(i, version, repSetUpdates.get(i)));
            } else if (i >= lut.replicationSets().size()) {
                actions.add(new PutReplicationSet(i, 0, null)); // can't leave empty spaces
            }
        }
    }

    private void repGroupActions(List<Action> actions) {
        for (Entry<Key, Integer> e : repGroupUpdates.entrySet()) {
            actions.add(new PutReplicationGroup(e.getKey(), e.getValue()));
        }
    }
    
    private void schemaActions(List<Action> actions) {
        for (Entry<ByteBuffer, SingleSchema> e : schemaUpdates.entrySet()) {
            if (e.getValue() == null) {
                actions.add(new DropSchema(e.getKey()));
            } else {
                SingleSchema schema = e.getValue();
                actions.add(new CreateSchema(schema.id.array(), schema.name, schema.meta));
            }
        }
    }

    public ArrayList<Integer> createRepSets(int rfactor) {
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
        List<Integer> nats = new ArrayList<Integer>();
        int highestHostId = Math.max(hostUpdates.size(), lut.hosts().size());
        for (int i = 0; i < highestHostId; i++) {
            if (getHost(i) != null) {
                nats.add(i);
            }
        }
        HashSet<TreeSet<Integer>> copysets = new HashSet<TreeSet<Integer>>();
        int p = 0;
        while (p < numberOfPermutations) {
            List<Integer> perm = new ArrayList<Integer>(nats);
            Collections.shuffle(perm, LookupTable.RAND);
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
        ArrayList<Integer> res = new ArrayList<Integer>();
        for (TreeSet<Integer> copyset : copysets) {
            Integer[] set = new Integer[copyset.size()];
            copyset.toArray(set);
            int rsId = nextRSId();
            res.add(rsId);
            putRepSet(rsId, set);
        }
        return res;
    }

    private int nextRSId() {
        return Math.max(lut.replicationSets().size()-1, repSetUpdates.lastKey()) + 1;
    }
}
