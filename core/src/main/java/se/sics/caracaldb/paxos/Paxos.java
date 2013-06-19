/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.primitives.Ints;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.View;
import se.sics.caracaldb.paxos.leader.LeaderDetector;
import se.sics.caracaldb.paxos.leader.Omega;
import se.sics.caracaldb.paxos.leader.OmegaInit;
import se.sics.caracaldb.paxos.leader.Trust;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stopped;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Paxos extends ComponentDefinition {

    public static enum State {

        ACTIVE,
        PASSIVE;
    }
    private static final Logger LOG = LoggerFactory.getLogger(Paxos.class);
    // Ports & Components
    Negative<Consensus> consensus = provides(Consensus.class);
    Positive<LeaderDetector> eld = requires(LeaderDetector.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    Component omega;
    private final Address self;
    // Instance
    private State state = State.ACTIVE;
    private int quorum;
    private boolean leader = true;
    private View view;
    private SortedSet<Decide> proposals = new TreeSet<Decide>();
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



        omega = create(Omega.class, new OmegaInit(init.networkBound / 10, init.networkBound, self));
        connect(omega.getPositive(LeaderDetector.class), eld.getPair());
        connect(omega.getNegative(Network.class), net);
        connect(omega.getNegative(Timer.class), timer);

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

        subscribe(proposeHandler, consensus);

        subscribe(trustHandler, eld);

        subscribe(prepareHandler, net);
        subscribe(promiseHandler, net);
        subscribe(nopromiseHandler, net);
        subscribe(acceptHandler, net);
        subscribe(acceptedHandler, net);
        subscribe(rejectedHandler, net);
        subscribe(forwardHandler, net);
    }
    Handler<Install> installHandler = new Handler<Install>() {
        @Override
        public void handle(Install event) {
            LOG.info("{}: Got install for {} and ({}, {})", 
                    new Object[] {self, event.event, event.ballot, event.highestDecided});
            Reconfigure rconf = event.event;
            view = rconf.view;
            quorum = rconf.quorum;
            bal = event.ballot;
            highestDecidedId = event.highestDecided;

            goActive();
            trigger(rconf, eld);
            trigger(rconf, consensus);
            unsubscribe(this, net);
        }
    };
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            trigger(new Reconfigure(view, quorum), eld);
        }
    };
    Handler<Stopped> stoppedHandler = new Handler<Stopped>() {
        @Override
        public void handle(Stopped event) {

            disconnect(omega.getPositive(LeaderDetector.class), eld.getPair());
            disconnect(omega.getNegative(Network.class), net);
            disconnect(omega.getNegative(Timer.class), timer);
            destroy(omega);
        }
    };
    // ANY
    Handler<Propose> proposeHandler = new Handler<Propose>() {
        @Override
        public void handle(Propose event) {
            LOG.debug("Got Propose({}) for {}", event.id, event.event);
            forwardPropose(event.event);
        }
    };
    Handler<Forward> forwardHandler = new Handler<Forward>() {
        @Override
        public void handle(Forward event) {
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
    // ACCEPTOR
    Handler<Prepare> prepareHandler = new Handler<Prepare>() {
        @Override
        public void handle(Prepare event) {
            if (!view.members.contains(event.getSource())) {
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
            Set<Decide> recentlyProposed = new HashSet<Decide>();
            while (!instanceIds.isEmpty() && (lastInstance < instanceIds.last())) {
                lastInstance++;
                SortedSet<Instance> iS = val2a.removeAll(lastInstance);
                if (!iS.isEmpty()) {
                    Instance i = iS.last(); // instance with highest ballot
                    phase2a(i.id, b, i.value);
                    recentlyProposed.add(i.value);
                } else {
                    // fill gaps with noops
                    phase2a(lastInstance, b, new Noop());
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
            if (lastProposedId < lastInstance) {
                lastProposedId = lastInstance;
            }
            LOG.debug("{}: Proposing new values.", self);
            // Propose new values on free instances
            //ImmutableSet<Decide> props = ImmutableSet.copyOf(proposals);
            // for the record: I hate concurrent modification exceptions -.-
            for (Decide p : proposals) {
                if (!recentlyProposed.contains(p)) {
                    lastProposedId++;
                    phase2a(lastProposedId, b, p);
                } // simply avoid proposing the same thing twice in a row
            }
//            while (!proposals.isEmpty()) {
//                Decide p = proposals.first();
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

    private void phase2a(long id, int ballot, Decide value) {
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

    private void propose(Decide p) {
        if (leader) {
            if (prepared) {
                // steady state
                lastProposedId++;
                phase2a(lastProposedId, b, p);
            }
        }
    }

    private void forwardPropose(Decide p) {
        //LOG.debug("Forwarding {} to {}", p, curLeader);
        LOG.debug("Forwarding {}", p);
        // Forward to everyone to avoid message losses when leader fails
        for (Address adr : view.members) {
            trigger(new Forward(self, adr, p), net);
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
        Decide value = i.value;

        highestDecidedId = i.id;
        acceptedInstances.removeAll(i.id);
        acceptedSet.removeAll(i);
        votes.remove(i.id);
        if (value instanceof Noop) {
            LOG.debug("{}: Decided instance {} with Noop", self, i);
            // do nothing^^
            return;
        }
        if (!proposals.remove(value)) {
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
                trigger(new Install(self, adr, b, rconf, highestDecidedId), net);
            }
            trigger(rconf, eld);
            view = rconf.view;
            quorum = rconf.quorum;
            if (view.members.size() < quorum) {
                LOG.warn("{}: Reconfiguring with less nodes in group than "
                        + "required for quorum (Group: {} - Quorum: {})",
                        new Object[]{self, view.members.size(), quorum});
            }

        }
        trigger(value, consensus);
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

    // Other Classes
    public static class Forward extends Message {

        public final Decide p;

        public Forward(Address src, Address dst, Decide p) {
            super(src, dst);
            this.p = p;
        }
    }

    public static class Instance implements Comparable<Instance> {

        public final long id;
        public final int ballot;
        public final Decide value;

        public Instance(long id, int ballot, Decide value) {
            this.id = id;
            this.ballot = ballot;
            this.value = value;
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
            return new Instance(id, ballot, new Noop());
        }
    }

    public static class Noop extends Decide {

        @Override
        public int compareTo(Decide o) {
            if (o instanceof Noop) {
                return 0;
            }
            return super.baseCompareTo(o);
        }
    }

    // Messages
    public static abstract class PaxosMsg extends Message {

        public final int ballot;

        public PaxosMsg(Address src, Address dst, int ballot) {
            super(src, dst);
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

        public Install(Address src, Address dst, int ballot, Reconfigure event, long highestDecided) {
            super(src, dst, ballot);
            this.event = event;
            this.highestDecided = highestDecided;
        }
    }
}
