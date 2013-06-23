/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos.leader;

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.paxos.Reconfigure;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Stop;
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
public class Omega extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(Omega.class);
    // Ports
    Negative<LeaderDetector> eld = provides(LeaderDetector.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    // Immutable
    private final Address self;
    private final long delta;
    // Instance    
    private long timeout;
    private Set<Address> nodes;
    private SortedSet<Address> candidates;
    private Address leader;
    private UUID timerId;
    private long currentId = 0;

    public Omega(OmegaInit init) {
        this.delta = init.delta;
        this.timeout = init.timeDelay;
        self = init.self;

        nodes = new HashSet<Address>();
        candidates = new TreeSet<Address>();

        // subscriptions
        subscribe(reconfHandler, eld);
        subscribe(timeoutHandler, timer);
        subscribe(heartbeatHandler, net);
    }
//    Handler<Start> startHandler = new Handler<Start>() {
//
//        @Override
//        public void handle(Start event) {
//            
//        }
//    };
    Handler<Reconfigure> reconfHandler = new Handler<Reconfigure>() {
        @Override
        public void handle(Reconfigure event) {
            if (event.view.members.size() == 0) {
                LOG.info("Reconfiguring to an empty state!");
                nodes.clear();
                candidates.clear();
                if (timerId != null) {
                    trigger(new CancelPeriodicTimeout(timerId), timer);
                    timer = null;
                }
                currentId++;
                return;
            }
            nodes.clear();
            candidates.clear();
            nodes.addAll(event.view.members);
            candidates.addAll(event.view.members);
            leader = select();
            trigger(new Trust(leader), eld);

            broadcast();
            candidates.clear();
            changeTimer();
        }
    };
    Handler<OmegaTimeout> timeoutHandler = new Handler<OmegaTimeout>() {
        @Override
        public void handle(OmegaTimeout event) {
            if (event.id != currentId) {
                return; // ignore old timeouts
            }
            if (!event.getTimeoutId().equals(timerId)) {
                LOG.warn("{}: Got wrong timeout {} - expected {}", new Object[] {self, event.getTimeoutId(), timerId});
                return;
            }
            if (candidates.isEmpty()) {
                timeout = timeout + delta;
                changeTimer();
                if (!leader.equals(self)) {
                    trigger(new Trust(self), eld);
                }
                return;
            }
            Address newLeader = select();
            if (!leader.equals(newLeader) && newLeader != null) {
                timeout = timeout + delta;
                leader = newLeader;
                trigger(new Trust(leader), eld);
                changeTimer();
            }
            broadcast();
            candidates.clear();
        }
    };
    Handler<Heartbeat> heartbeatHandler = new Handler<Heartbeat>() {
        @Override
        public void handle(Heartbeat event) {
            //LOG.debug("{}: Got Heartbeat from {}", self, event.getSource());
            if (!nodes.contains(event.getSource())) {
                return; // ignore foreign heatbeats
            }
            candidates.add(event.getSource());
        }
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            if (timerId != null) {
                trigger(new CancelPeriodicTimeout(timerId), timer);
            }
        }
    };

    private Address select() {
        return candidates.first();
    }

    private void broadcast() {
        for (Address adr : nodes) {
            trigger(new Heartbeat(self, adr), net);
        }
    }

    private void changeTimer() {
        if (timerId != null) {
            trigger(new CancelPeriodicTimeout(timerId), timer);
        }
        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(timeout, timeout);
        currentId++;
        OmegaTimeout ot = new OmegaTimeout(spt, currentId);
        spt.setTimeoutEvent(ot);
        trigger(spt, timer);
        timerId = ot.getTimeoutId();
    }

    public static class Heartbeat extends Message {

        public Heartbeat(Address src, Address dst) {
            super(src, dst);
        }

        @Override
        public String toString() {
            return "HB(" + this.getSource().toString() + ", " + this.getDestination().toString() + ")";
        }
    }

    public static class OmegaTimeout extends Timeout {

        public final long id;

        public OmegaTimeout(SchedulePeriodicTimeout spt, long id) {
            super(spt);
            this.id = id;
        }
    }
}
