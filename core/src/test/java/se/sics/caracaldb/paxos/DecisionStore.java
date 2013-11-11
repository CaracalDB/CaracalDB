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
package se.sics.caracaldb.paxos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.Assert;
import se.sics.caracaldb.paxos.PaxosManager.PaxosOp;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DecisionStore {

    private TreeMap<Integer, EpochStore> stores = new TreeMap<Integer, EpochStore>();
    private SortedMap<Long, Address> ops = new TreeMap<Long, Address>();
    private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
    private Set<Address> failed = new HashSet<Address>();
    private Set<Long> decided = new TreeSet<Long>();
    private Set<Address> currentGroup = new HashSet<Address>();

    public DecisionStore(ImmutableSet<Address> group) {
        currentGroup.addAll(group);
        stores.put(0, new EpochStore(0, group));
    }

    public void proposed(Address node, PaxosOp op) {
        rwlock.readLock().lock();
        try {
            ops.put(op.id, node);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    public void decided(int epoch, Address node, long value) {
        rwlock.readLock().lock();
        try {

            EpochStore store = stores.get(epoch);
            if (store == null) {
                throw new RuntimeException(node + " is trying to write into non-existant epoch " + epoch);
            }
            store.decided(node, value);
            decided.add(value);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    public int join(Address node) {
        rwlock.readLock().lock();
        try {
            int oldEpoch = stores.lastKey();
            EpochStore oldStore = stores.get(oldEpoch);
            Set<Address> newGroup = new HashSet<Address>(oldStore.active);
            newGroup.add(node);
            stores.put(oldEpoch + 1, new EpochStore(oldEpoch + 1, ImmutableSet.copyOf(newGroup)));

            return oldEpoch;

        } finally {
            rwlock.readLock().unlock();
        }

    }
    
    public void joined(Address node) {
        currentGroup.add(node);
    }

    public void fail(int epoch, Address node) {
        rwlock.readLock().lock();
        try {
            currentGroup.remove(node);
            EpochStore store = stores.get(epoch);
            store.decided(node, -1l);
            failed.add(node);
        } finally {
            rwlock.readLock().unlock();
        }
    }

    public void validate() {
        rwlock.writeLock().lock();
        try {
            /*
             * This is multiPaxos for replication so the validation is a 
             * little different from single instance classical paxos.
             * Basically if you stop after the first Decide event it's conensus, 
             * and if you keep it running it's atomic broadcast.
             * 
             * There are three things to show (reformulated a bit):
             * 1) Decided values have been proposed
             * 2) A value proposed by a correct node is eventually decided
             * 3) A value is only decided once 
             *  (Temporarily not in effect!!!!
             *  Turns out exactly once semantics is pretty hard with bounded history size)
             * 4) Every correct process delivers the same messages in the same order
             * 
             * Failed (i.e. non-correct/faulty) processes are marked 
             * with a -1 as last message in the store.
             * 
             */
            SortedSet<Long> validateOps = new TreeSet<Long>(ops.keySet()); // take a copy
            ImmutableSortedSet<Long> readOnlyOps = ImmutableSortedSet.copyOf(ops.keySet());
            for (EpochStore store : stores.values()) {
                /*
                 * Since this is a reconfigurable implementation (4) only needs
                 * to be shown per epoch since newly joined nodes can't know
                 * about old decisions (i.e. there's no replaying)
                 */
                store.validate(validateOps, readOnlyOps);
            }
            //Assert.assertTrue("Violated property (2)", validateOps.isEmpty());
            if (!validateOps.isEmpty()) {
                for (Long val : validateOps) {
                    Address proposer = ops.get(val);
                    Assert.assertTrue("Violated property (2)", failed.contains(proposer));
                }
            }
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    public int numOps() {
        return ops.size();
    }
    
    public boolean allDecided() {
        return decided.size() == ops.size();
    }

    public void html(StringBuilder sb) {
        rwlock.writeLock().lock();
        try {
            sb.append("<!DOCTYPE html>");
            sb.append("<html>");
            sb.append("<head><meta charset=\"utf-8\"/>");
            sb.append("<style>");
            sb.append("table { border-collapse: collapse; }");
            sb.append("table, th, td { border: 1px solid black; }");
            sb.append("</style>");
            sb.append("</head>");
            sb.append("<body>");

            sb.append("<h1>Paxos Test Run</h1>");
            sb.append("<h2>Proposed Values</h2>");
            sb.append("<table>");
            sb.append("<tr>");
            for (Long l : ops.keySet()) {
                sb.append("<td>");
                sb.append(l);
                sb.append("</td>");
            }
            sb.append("</tr>");
            sb.append("</table>");

            for (EpochStore store : stores.values()) {
                sb.append("<h2>Epoch ");
                sb.append(store.epoch);
                sb.append("</h2>");
                store.html(sb);
            }

            sb.append("</body>");
            sb.append("</html>");
        } finally {
            rwlock.writeLock().unlock();
        }
    }

    public static class EpochStore {

        private final int epoch;
        private LinkedListMultimap<Address, Long> decidedMap = LinkedListMultimap.create();
        public final ImmutableSet<Address> group;
        private final Set<Address> active;

        public EpochStore(int epoch, ImmutableSet<Address> group) {
            this.epoch = epoch;
            this.group = group;
            this.active = new HashSet<Address>(group);
        }

        public void decided(Address node, long value) {
            if (group.contains(node)) {
                decidedMap.put(node, value);
                if (value < 0) {
                    active.remove(node);
                }
            } else {
                //throw new RuntimeException("Can't write into an epoch where node is not present!");
                
            }
        }

        public void validate(SortedSet<Long> validateOps, ImmutableSortedSet<Long> ops) {
            Map<Address, Iterator<Long>> its = new HashMap<Address, Iterator<Long>>();
            //Map<Address, Long> lastValue = new HashMap<Address, Long>();
            for (Entry<Address, Collection<Long>> e : decidedMap.asMap().entrySet()) {
                its.put(e.getKey(), e.getValue().iterator());
            }
            while (!its.isEmpty()) {
                Long val = null;
                boolean first = true;
                boolean end = false;
                // copy to avoid concurrent modification exceptions
                ImmutableSet<Entry<Address, Iterator<Long>>> entrySet = ImmutableSet.copyOf(its.entrySet());
                for (Entry<Address, Iterator<Long>> e : entrySet) {
                    Address adr = e.getKey();
                    Iterator<Long> it = e.getValue();
                    if (it.hasNext()) {
                        long iVal = it.next();
                        //lastValue.put(adr, iVal);
                        if (iVal >= 0) {
                            if (first) {
                                first = false;
                                val = iVal;
                                Assert.assertTrue("Violated property (1): Missing: " + val, ops.contains(val));
                                // TODO see above (3)
                                validateOps.remove(val);
                                //Assert.assertTrue("Violated property (3): Already decided " + val, validateOps.remove(val));
                            } else {
                                if (!end) {
                                    Assert.assertEquals("Violated propery (4)", (long) val, iVal);
                                } else {
                                    Assert.fail("Violated propery (4): Some process didn't decide " + iVal);
                                }
                            }
                        } else {
                            if (it.hasNext()) {
                                // This is technically not a property violation,
                                // but it shouldn't happen either
                                Assert.fail("Process " + adr + " decided something after it failed.");
                            } else {
                                its.remove(adr);
                            }
                        }
                    } else {
                        if (first) {
                            first = false;
                            end = true;
                        } else {
                            if (!end) {
                                Assert.fail("Violated propery (4): Process " + adr + " didn't decide " + val + " without failing.");
                            }
                        }
                    }
                }
                if (end) {
                    break;
                }
            }
        }

        public void html(StringBuilder sb) {
            sb.append("<table>");



            Map<Address, Iterator<Long>> its = new HashMap<Address, Iterator<Long>>();
            //Map<Address, Long> lastValue = new HashMap<Address, Long>();
            for (Entry<Address, Collection<Long>> e : decidedMap.asMap().entrySet()) {
                its.put(e.getKey(), e.getValue().iterator());
            }


            Multimap<Address, String> lines = LinkedListMultimap.create();
            for (Address adr : group) {
                String colour = "";
                if (active.contains(adr)) {
                    colour = "<font style=\"color: green\">";
                } else {
                    colour = "<font style=\"color: red\">";
                }

                lines.put(adr, "<td><b>" + colour + adr.toString() + "</font></b></td>");
            }

            int pos = 0;
            while (!its.isEmpty()) {
                pos++;
                //System.out.println("Printing decision " + pos + " in epoch " + epoch + " its size: " + its.size());
                for (Address adr : group) {
                    Iterator<Long> it = its.get(adr);
                    if (it != null) {
                        if (it.hasNext()) {
                            Long val = it.next();
                            if (val < 0) {
                                lines.put(adr, "<td>X</td>");
                            } else {
                                lines.put(adr, "<td>" + val + "</td>");
                            }
                        } else {
                            its.remove(adr);
                            lines.put(adr, "<td>?</td>");
                        }
                    } else {
                        lines.put(adr, "<td>?</td>");
                    }
                }
            }

            sb.append("<tr>");
            sb.append("<td><b>Node/Decision#</b></td>");
            for (int i = 0; i < pos; i++) {
                sb.append("<td><b>");
                sb.append(i);
                sb.append("</b></td>");
            }
            sb.append("</tr>");

            for (Address adr : group) {
                sb.append("<tr>");
                Collection<String> line = lines.get(adr);
                for (String cell : line) {
                    sb.append(cell);
                }

                sb.append("</tr>");
            }
            sb.append("</table>");
        }
    }
}
