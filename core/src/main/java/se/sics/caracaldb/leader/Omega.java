/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.leader;

import com.google.common.collect.Sets.SetView;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.View;
import se.sics.caracaldb.paxos.Reconfigure;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.fd.Restore;
import se.sics.caracaldb.fd.SubscribeNodeStatus;
import se.sics.caracaldb.fd.Suspect;
import se.sics.caracaldb.fd.UnsubscribeNodeStatus;
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
    Positive<EventualFailureDetector> fd = requires(EventualFailureDetector.class);
    // Immutable
    private final Address self;
    // Instance   
    private View view = null;
    private SortedSet<Address> candidates = new TreeSet<Address>();
    private Address leader;
    private HashMap<Address, UUID> probes = new HashMap<Address, UUID>();

    public Omega(OmegaInit init) {
        self = init.self;

        // subscriptions
        subscribe(reconfHandler, eld);
        subscribe(restoreHandler, fd);
        subscribe(suspectHandler, fd);
        subscribe(stopHandler, control);
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
                candidates.clear();
                if (!probes.isEmpty()) {
                    for (Entry<Address, UUID> e : probes.entrySet()) {
                        Address adr = e.getKey();
                        UUID id = e.getValue();
                        trigger(new UnsubscribeNodeStatus(id, adr), fd);
                    }
                }
                probes.clear();
                view = event.view;
                return;
            }
            LOG.debug("{}: Reconfiguring from {} to {}", new Object[] {self, view, event.view});
            candidates.clear();
            candidates.addAll(event.view.members);
            // remove old probes
            if (view != null) {
                for (Address adr : event.view.removedSince(view)) {
                    UUID id = probes.get(adr);
                    trigger(new UnsubscribeNodeStatus(id, adr), fd);
                    probes.remove(adr);
                }

                // add new probes
                for (Address adr : event.view.addedSince(view)) {
                    SubscribeNodeStatus sub = new SubscribeNodeStatus(adr);
                    trigger(sub, fd);
                    probes.put(adr, sub.requestId);
                }
            } else {
                for (Address adr : event.view.members) {
                    SubscribeNodeStatus sub = new SubscribeNodeStatus(adr);
                    trigger(sub, fd);
                    probes.put(adr, sub.requestId);
                }
            }
            leader = select();
            trigger(new Trust(leader), eld);

            view = event.view;
        }
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            if (!probes.isEmpty()) {
                for (Entry<Address, UUID> e : probes.entrySet()) {
                    Address adr = e.getKey();
                    UUID id = e.getValue();
                    trigger(new UnsubscribeNodeStatus(id, adr), fd);
                }
            }
            probes.clear();
            candidates.clear();
            LOG.debug("{}: Stopping", self);
        }
    };
    Handler<Suspect> suspectHandler = new Handler<Suspect>() {
        @Override
        public void handle(Suspect event) {
            LOG.debug("{}: Suspecting {}", self, event.node);
            if (!view.members.contains(event.node)) {
                return; //ignore message from old view
            }
            candidates.remove(event.node);
            Address newLeader = select();
            if (leader != newLeader) {
                leader = newLeader;
                trigger(new Trust(leader), eld);
            }
        }
    };
    Handler<Restore> restoreHandler = new Handler<Restore>() {
        @Override
        public void handle(Restore event) {
            LOG.debug("{}: Restoring {}", self, event.node);
            if (!view.members.contains(event.node)) {
                return; //ignore message from old view
            }
            candidates.add(event.node);
            Address newLeader = select();
            if (leader != newLeader) {
                leader = newLeader;
                trigger(new Trust(leader), eld);
            }
        }
    };

    private Address select() {
        return candidates.first();
    }
}
