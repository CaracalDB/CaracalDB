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
package se.sics.caracaldb.leader;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.View;
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
    Handler<ReconfigureGroup> reconfHandler = new Handler<ReconfigureGroup>() {
        @Override
        public void handle(ReconfigureGroup event) {
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
            LOG.debug("{}: Reconfiguring from {} to {}", new Object[]{self, view, event.view});
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
            notifyListeners();
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
            notifyListeners();
        }
    };

    private Address select() {
        return candidates.first();
    }

    private void notifyListeners() {
        Address newLeader = select();
        if (leader != newLeader) {
            leader = newLeader;
            trigger(new Trust(leader), eld);
        } else {
            trigger(GroupStatusChange.EVENT, eld);
        }
    }
}
