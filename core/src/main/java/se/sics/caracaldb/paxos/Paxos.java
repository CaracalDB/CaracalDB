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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.Ints;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.BaseMessage;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.View;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.leader.GroupStatusChange;
import se.sics.caracaldb.leader.LeaderDetector;
import se.sics.caracaldb.leader.Omega;
import se.sics.caracaldb.leader.OmegaInit;
import se.sics.caracaldb.leader.ReconfigureGroup;
import se.sics.caracaldb.leader.Trust;
import se.sics.caracaldb.replication.log.Decide;
import se.sics.caracaldb.replication.log.Noop;
import se.sics.caracaldb.replication.log.Propose;
import se.sics.caracaldb.replication.log.Prune;
import se.sics.caracaldb.replication.log.Reconfigure;
import se.sics.caracaldb.replication.log.ReplicatedLog;
import se.sics.caracaldb.replication.log.Value;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stopped;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.network.netty.serialization.Serializers;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Paxos extends ComponentDefinition {

    static {
        Serializers.register(CoreSerializer.PAXOS.instance, "paxosS");
        Serializers.register(PaxosMsg.class, "paxosS");
        Serializers.register(Forward.class, "paxosS");
        Serializers.register(CoreSerializer.VALUE.instance, "valueS");
        Serializers.register(Value.class, "valueS");
    }

    public static enum State {

        ACTIVE,
        PASSIVE;
    }
    private static final Logger LOG = LoggerFactory.getLogger(Paxos.class);
    // Ports & Components
    Negative<ReplicatedLog> rLog = provides(ReplicatedLog.class);
    Positive<LeaderDetector> eld = requires(LeaderDetector.class);
    Negative<LeaderDetector> eldPass = provides(LeaderDetector.class);
    Positive<Network> net = requires(Network.class);
    Positive<EventualFailureDetector> fd = requires(EventualFailureDetector.class);
    //Positive<Timer> timer = requires(Timer.class); //TODO check if necessary, connections
    Component omega;
    private final Address self;
    // Instance
    private State state = State.ACTIVE;
    private int quorum;
    private boolean leader = true;
    private View view;
    private SortedSet<Value> proposals = new TreeSet<Value>();
    private TreeMap<Long, Value> decidedLog = new TreeMap<Long, Value>();
    // ACCEPTOR
    // maintains maxbal(i) and maxvote(i) for each instance i
    private SortedMap<Long, Instance> votes = new TreeMap<Long, Instance>();
    private int bal;
    // LEADER
    private TreeMultimap<Long, Instance> val2a = TreeMultimap.create();
    private Map<Address, Promise> prepareSet = new HashMap<Address, Promise>();
    private boolean prepared = false;
    private long lastProposedId = -1;
    private int b; // like bal just for leader
    //private Queue<Decide> proposeQ = new LinkedList<Decide>();
    // LEARNER
    private SortedSetMultimap<Instance, Accepted> acceptedSet = TreeMultimap.create();
    private SortedSetMultimap<Long, Instance> acceptedInstances = TreeMultimap.create();
    private long highestDecidedId = -1;

    public Paxos(PaxosInit init) {
        self = init.self;
        view = init.view;
        quorum = init.quorum;
        bal = 0;
        b = 0;

        omega = create(Omega.class, new OmegaInit(self));
        connect(omega.getPositive(LeaderDetector.class), eld.getPair());
        connect(omega.getPositive(LeaderDetector.class), eldPass);
        connect(omega.getNegative(EventualFailureDetector.class), fd);

        // Subscriptions
        subscribe(stoppedHandler, control);

        if (view != null) {
            goActive();
        } else {
            subscribe(installHandler, net);
        }
    }

    private void goActive() {
        state = State.ACTIVE;

        subscribe(startHandler, control);

        subscribe(proposeHandler, rLog);

        subscribe(trustHandler, eld);
        subscribe(gsChangeHandler, eld);

        subscribe(prepareHandler, net);
        subscribe(promiseHandler, net);
        subscribe(nopromiseHandler, net);
        subscribe(acceptHandler, net);
        subscribe(acceptedHandler, net);
        subscribe(rejectedHandler, net);
        subscribe(forwardHandler, net);
        subscribe(pruneHandler, rLog);
    }
    Handler<Install> installHandler = new Handler<Install>() {
        @Override
        public void handle(Install event) {
            LOG.info("{}: Got install for {} and ({}, {})",
                    new Object[]{self, event.event, event.ballot, event.highestDecided});
            Reconfigure rconf = event.event;
            view = rconf.view;
            quorum = rconf.quorum;
            bal = event.ballot;
            highestDecidedId = event.highestDecided;

            goActive();
            trigger(toELDReconf(rconf), eld);
            trigger(new Decide(highestDecidedId, rconf), rLog);
            unsubscribe(this, net);
        }
    };
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            trigger(new ReconfigureGroup(view, quorum), eld);
        }
    };
    Handler<Stopped> stoppedHandler = new Handler<Stopped>() {
        @Override
        public void handle(Stopped event) {

            disconnect(omega.getPositive(LeaderDetector.class), eld.getPair());
            disconnect(omega.getNegative(EventualFailureDetector.class), fd);
            destroy(omega);
        }
    };
    Handler<Prune> pruneHandler = new Handler<Prune>() {

        @Override
        public void handle(Prune event) {
            // TODO find a structure that supports this operation better
            decidedLog = new TreeMap<Long, Value>(decidedLog.tailMap(lastProposedId, false));
        }
    };
    // ANY
    Handler<Propose> proposeHandler = new Handler<Propose>() {
        @Override
        public void handle(Propose event) {
            LOG.debug("Got Propose({})", event.value);
            forwardPropose(event.value);
        }
    };
    Handler<Forward> forwardHandler = new Handler<Forward>() {
        @Override
        public void handle(Forward event) {
            LOG.debug("{}: Got Forward {}", self, event.p);
            proposals.add(event.p);
            propose(event.p);
        }
    };
    Handler<Trust> trustHandler = new Handler<Trust>() {
        @Override
        public void handle(Trust event) {
            //curLeader = event.leader;
            leader = self.equals(event.leader);
            LOG.debug("{}: Got Trust({})", self, event.leader);
            collision(bal, false); // cheat and use acceptor ballot
        }
    };
    Handler<GroupStatusChange> gsChangeHandler = new Handler<GroupStatusChange>() {

        @Override
        public void handle(GroupStatusChange event) {
            LOG.debug("{}: Got GSC event.", self);
            if (leader && !prepared) {
                collision(bal, false); // try to prepare again and hope to get a majority this time
            }
        }
    };
    // ACCEPTOR
    Handler<Prepare> prepareHandler = new Handler<Prepare>() {
        @Override
        public void handle(Prepare event) {
            if (!view.members.contains(event.getSource())) {
                LOG.debug("{}: Ignoring Prepare({}) from {}. View is {}", new Object[]{self, event.ballot, event.getSource(), view});
                return; // ignore prepares from outsiders
            }
            LOG.debug("{}: Got Prepare({}) from {}",
                    new Object[]{self, event.ballot, event.getSource()});
            if (event.ballot > bal) {
                bal = event.ballot;
                trigger(new Promise(self, event.getSource(), bal,
                        ImmutableSet.copyOf(votes.values()), view), net);
            } else { // purely optimisation could just ignore the message
                trigger(new NoPromise(self, event.getSource(), bal), net);
            }
        }
    };
    Handler<Accept> acceptHandler = new Handler<Accept>() {
        @Override
        public void handle(Accept event) {
            if (bal <= event.ballot) {
                bal = event.ballot;
                accept(event.i);
            } else { // purely an optimisation
                reject(event.i, event.getSource());
            }
        }
    };
    // LEADER
    Handler<Promise> promiseHandler = new Handler<Promise>() {
        @Override
        public void handle(Promise event) {
            if (prepared) {
                return; // already got a quorum of promises before
            }
            LOG.debug("{}: Got Promise({}) from {}",
                    new Object[]{self, event.ballot, event.getSource()});

            if (event.ballot < b) {
                return; // these are old promises
            }
            if (!event.view.equals(view)) {
                LOG.warn("{}: Got promise in different view: {}", self, event.view);
                return; //ignore it
            }

            // No collision
            prepareSet.put(event.getSource(), event);
            for (Instance i : event.maxInstances) {
                val2a.put(i.id, i);
            }
            prepared = prepareSet.size() >= quorum;
            if (!prepared) {
                LOG.debug("{}: Waiting for more promises. Got {}, need {}", new Object[]{self, prepareSet.size(), quorum});
                return;
            }
            // Verify that the quorum is with the same view
            for (Entry<Address, Promise> e : prepareSet.entrySet()) {
                Promise p = e.getValue();
                if (!p.view.equals(view)) {
                    LOG.error("{}: Tried to assemble non-consistent quorum with different views.", self);
                    return;
                }
            }
            LOG.debug("{}: Moving to steady state", self);

            // Ready for Phase 2
            //ImmutableSet<Long> instanceIds = ImmutableSet.copyOf(activeInstances.keySet());
            long lastInstance = highestDecidedId;
            SortedSet<Long> instanceIds = val2a.keySet();
            Set<Value> recentlyProposed = new HashSet<Value>();
            while (!instanceIds.isEmpty() && (lastInstance < instanceIds.last())) {
                lastInstance++;
                SortedSet<Instance> iS = val2a.removeAll(lastInstance);
                if (!iS.isEmpty()) {
                    Instance i = iS.last(); // instance with highest ballot
                    phase2a(i.id, b, i.value);
                    recentlyProposed.add(i.value);
                } else {
                    // fill gaps with noops
                    phase2a(lastInstance, b, Noop.val);
                }
            }

//            for (Long instanceId : instanceIds) {
//                /*
//                 * val2a(i;b) is a command v, 
//                 * meaning that v may have been chosen in
//                 * instance i at some ballot less than b
//                 */
//                SortedSet<Instance> values = activeInstances.get(instanceId);
//                Instance max = values.last();
//                if (max.id <= highestDecidedId) {
//                    // already decided this instance
//                    // try again on another one
//                    proposeQ.offer(max.value);
//                    continue;
//                }
//                // fill gaps with noops                
//                while (max.id > (lastInstance + 1)) {
//                    lastInstance++;
//                    phase2a(Instance.noop(lastInstance, b));
//                }
//                phase2a(max);
//                lastInstance++;
//            }
            lastProposedId = lastInstance;

            LOG.debug("{}: Proposing new values.", self);
            // Propose new values on free instances
            //ImmutableSet<Decide> props = ImmutableSet.copyOf(proposals);
            // for the record: I hate concurrent modification exceptions -.-
            for (Value p : proposals) {
                if (!recentlyProposed.contains(p)) {
                    lastProposedId++;
                    phase2a(lastProposedId, b, p);
                } // simply avoid proposing the same thing twice in a row
            }
//            while (!proposals.isEmpty()) {
//                Value p = proposals.first();
//                lastProposedId++;
//                phase2a(new Instance(lastProposedId, bal, p));
//            }

        }
    };
    Handler<NoPromise> nopromiseHandler = new Handler<NoPromise>() {
        @Override
        public void handle(NoPromise event) {
            LOG.debug("{}: Got NoPromise({}) from {}",
                    new Object[]{self, event.ballot, event.getSource()});
            collision(event.ballot, true);

        }
    };
    Handler<Rejected> rejectedHandler = new Handler<Rejected>() {
        @Override
        public void handle(Rejected event) {
            LOG.debug("{}: Got rejected for Instance({}, {}, {})", new Object[]{self, event.i.id, event.i.ballot, event.i.value});
            collision(event.ballot, true);
        }
    };
    // LEARNER
    Handler<Accepted> acceptedHandler = new Handler<Accepted>() {
        @Override
        public void handle(Accepted event) {
            if ((highestDecidedId < event.i.id) && (view.id <= event.view.id)) {
                LOG.debug("{}: Got Accepted({}, {}, {}) from {}", new Object[]{self, event.i.id, event.i.ballot, event.i.value, event.getSource()});

                acceptedInstances.put(event.i.id, event.i);
                acceptedSet.put(event.i, event);
                decide();
                return;
            } // otherwise ignore since it's already been decided
            LOG.debug("{}: Ignoring Accepted({}, {}, {}, {}) from {} (decided up to ({}, {}))",
                    new Object[]{self, event.i.id, event.i.ballot, event.i.value, event.view.id, event.getSource(), highestDecidedId, view.id});

        }
    };

    private void collision(int ballot, boolean optimisation) {
        if (optimisation && (b > ballot)) {
            return; // Already handled this NACK
        }
        LOG.debug("{}: Collision: local {} - remote {}", new Object[]{self, b, ballot});
        clearLeaderState();
        if (leader) {
            if (b < ballot) {
                b = ballot; // just a shortcut to the right ballot
            }
            b++;
            prepare(b);
        }
    }

    private void prepare(int ballot) {
        LOG.debug("{}: Preparing ballot {}", self, ballot);
        for (Address adr : view.members) {
            trigger(new Prepare(self, adr, ballot), net);
        }
    }

    private void clearLeaderState() {
        val2a.clear();
        prepareSet.clear();
        prepared = false;
    }

    private void phase2a(long id, int ballot, Value value) {
        LOG.debug("{}: Executing Phase2A for Instance({}, {}, {})", new Object[]{self, id, ballot, value});
        Instance i = new Instance(id, ballot, value);
        for (Address adr : view.members) {
            trigger(new Accept(self, adr, ballot, i), net);
        }
    }

    private void accept(Instance i) {
        LOG.debug("{}: Voting for Instance({}, {}, {})", new Object[]{self, i.id, i.ballot, i.value});
        votes.put(i.id, i);
        for (Address adr : view.members) {
            trigger(new Accepted(self, adr, bal, i, view), net);
        }
    }

    private void reject(Instance i, Address src) {
        LOG.debug("{}: Rejecting Instance({}, {}, {})", new Object[]{self, i.id, i.ballot, i.value});

        trigger(new Rejected(self, src, bal, i), net);
    }

    private void propose(Value p) {
        if (leader) {
            if (prepared) {
                // steady state
                lastProposedId++;
                phase2a(lastProposedId, b, p);
            } else {
                LOG.debug("{}: Is leader, but not prepared. Set: {}", self, prepareSet);
                collision(bal, false); // FIXME this is very risky...at high op rates steady state might never be reached
            }
        }
    }

    private void forwardPropose(Value p) {
        //LOG.debug("Forwarding {} to {}", p, curLeader);
        LOG.debug("Forwarding {}", p);
        // Forward to everyone to avoid message losses when leader fails
        for (Address adr : view.members) {
            trigger(new Forward(self, adr, self, p), net);
        }

    }

    private void decide() {
        ImmutableSet<Long> instances = ImmutableSet.copyOf(acceptedInstances.keySet());
        for (Long iId : instances) {
            Instance i = acceptedInstances.get(iId).last(); // the one with the highest ballot
            View v = consistentQuorum(i);
            if (v != null) {
                if (!v.equals(view)) {
                    LOG.warn("{}: Got a consistent quorum for {} in view {} instead of {}",
                            new Object[]{self, i, v, view});
                    return;
                }
                if (i.id != highestDecidedId + 1) {
                    LOG.warn("{}: Trying to decide instance {} but last decided was {}",
                            new Object[]{self, i, highestDecidedId});
                    return;
                }
                decide(i);
            }
        }
    }

    private void decide(Instance i) {
        Value value = i.value;

        highestDecidedId = i.id;
        acceptedInstances.removeAll(i.id);
        acceptedSet.removeAll(i);
        votes.remove(i.id);
//        if (value instanceof Noop) {
//            LOG.debug("{}: Decided instance {} with Noop", self, i);
//            // do nothing^^
//            return;
//        }
        if (!proposals.remove(value) && !(value instanceof Noop)) {
            LOG.warn("{}: Decided value was not in proposals: {}", self, value);
        }
        if (value instanceof Reconfigure) {
            Reconfigure rconf = (Reconfigure) value;
            if (rconf.view.compareTo(view) < 0) {
                LOG.error("{}: Reconfiguring from {} to outdated view: {}",
                        new Object[]{self, view, rconf.view});
                //return; // already installed this view
            }
            for (Address adr : Sets.difference(rconf.view.members, view.members)) {
                // for all newly added nodes
                trigger(new Install(self, adr, b, rconf, highestDecidedId, ImmutableSortedMap.copyOf(decidedLog)), net);
            }
            trigger(toELDReconf(rconf), eld);
            view = rconf.view;
            quorum = rconf.quorum;
            if (view.members.size() < quorum) {
                LOG.warn("{}: Reconfiguring with less nodes in group than "
                        + "required for quorum (Group: {} - Quorum: {})",
                        new Object[]{self, view.members.size(), quorum});
            }

        }
        trigger(new Decide(i.id, value), rLog);
        LOG.debug("{}: Decided {}", self, value);
    }

    private View consistentQuorum(Instance i) {
        SortedSet<Accepted> acceptors = acceptedSet.get(i);
        if (acceptors.size() < quorum) {
            return null; // can abort early here
        }
        TreeMultimap<View, Address> quorums = TreeMultimap.create();
        for (Accepted acc : acceptors) {
            quorums.put(acc.view, acc.getSource());
            if (quorums.get(acc.view).size() >= quorum) {
                return acc.view;
            }
        }
        return null;
    }

    private ReconfigureGroup toELDReconf(Reconfigure r) {
        return new ReconfigureGroup(r.view, r.quorum);
    }

    // Other Classes
    public static class Forward extends BaseMessage {
        
        public final Value p;

        public Forward(Address src, Address dst, Address orig, Value p) {
            super(src, dst, orig, Transport.TCP);
            this.p = p;
        }
    }

    public static class Instance implements Comparable<Instance> {

        public final long id;
        public final int ballot;
        public final Value value;

        public Instance(long id, int ballot, Value value) {
            this.id = id;
            this.ballot = ballot;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Instance) {
                Instance that = (Instance) o;
                return this.compareTo(that) == 0;
            }
            return false;
        }
        
        @Override
        public int compareTo(Instance that) {
            if (id != that.id) {
                long r = id - that.id;
                return Ints.checkedCast(r);
            }
            if (ballot != that.ballot) {
                return ballot - that.ballot;
            }
            return value.compareTo(that.value);
        }

        @Override
        public String toString() {
            return "Instance(" + id + ", " + ballot + ", " + value.toString() + ")";
        }

        public static Instance noop(long id, int ballot) {
            return new Instance(id, ballot, Noop.val);
        }
    }

    // Messages
    public static abstract class PaxosMsg extends BaseMessage {

        public final int ballot;

        public PaxosMsg(Address src, Address dst, int ballot) {
            super(src, dst, Transport.TCP);
            this.ballot = ballot;
        }
    }

    public static class Prepare extends PaxosMsg {

        public Prepare(Address src, Address dst, int ballot) {
            super(src, dst, ballot);
        }
    }

    public static class Promise extends PaxosMsg {

        public final ImmutableSet<Instance> maxInstances;
        public final View view;

        public Promise(Address src, Address dst, int ballot, ImmutableSet<Instance> maxInstances, View v) {
            super(src, dst, ballot);
            this.maxInstances = maxInstances;
            this.view = v;
        }
    }

    public static class NoPromise extends PaxosMsg {

        public NoPromise(Address src, Address dst, int ballot) {
            super(src, dst, ballot);
        }
    }

    public static class Accept extends PaxosMsg {

        public final Instance i;

        public Accept(Address src, Address dst, int ballot, Instance i) {
            super(src, dst, ballot);
            this.i = i;
        }
    }

    public static class Accepted extends PaxosMsg implements Comparable<Accepted> {

        public final Instance i;
        public final View view;

        public Accepted(Address src, Address dst, int ballot, Instance i, View view) {
            super(src, dst, ballot);
            this.i = i;
            this.view = view;
        }

        @Override
        public int compareTo(Accepted that) {
            int diff = this.getSource().compareTo(that.getSource());
            if (diff != 0) {
                return diff;
            }
            diff = this.getDestination().compareTo(that.getDestination());
            if (diff != 0) {
                return diff;
            }
            if (this.ballot != that.ballot) {
                return this.ballot - that.ballot;
            }
            diff = this.i.compareTo(that.i);
            if (diff != 0) {
                return diff;
            }
            diff = this.view.compareTo(that.view);
            if (diff != 0) {
                return diff;
            }
            return 0;
        }
    }

    public static class Rejected extends PaxosMsg {

        public final Instance i;

        public Rejected(Address src, Address dst, int ballot, Instance i) {
            super(src, dst, ballot);
            this.i = i;
        }
    }

    public static class Install extends PaxosMsg {

        public final Reconfigure event;
        public final long highestDecided;
        public final ImmutableSortedMap<Long, Value> log;

        public Install(Address src, Address dst, int ballot, Reconfigure event, long highestDecided, ImmutableSortedMap<Long, Value> log) {
            super(src, dst, ballot);
            this.event = event;
            this.highestDecided = highestDecided;
            this.log = log;
        }
    }
}
