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
package se.sics.caracaldb.operations;

import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.global.ForwardToAny;
import se.sics.caracaldb.global.ForwardToRange;
import se.sics.caracaldb.global.LookupService;
import se.sics.caracaldb.global.MaintenanceMsg;
import se.sics.caracaldb.global.MaintenanceService;
import se.sics.caracaldb.global.NodeStats;
import se.sics.caracaldb.global.NodeSynced;
import se.sics.caracaldb.global.Reconfiguration;
import se.sics.caracaldb.replication.linearisable.Replication;
import se.sics.caracaldb.replication.linearisable.ReplicationSetInfo;
import se.sics.caracaldb.replication.linearisable.Synced;
import se.sics.caracaldb.store.Diff;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class MethCat extends ComponentDefinition {

    public static enum State {

        FORWARDING,
        ACTIVE;
    }
    private static final Logger LOG = LoggerFactory.getLogger(MethCat.class);
    Positive<Network> network = requires(Network.class);
    Positive<Replication> replication = requires(Replication.class);
    Positive<LookupService> lookup = requires(LookupService.class);
    Positive<MaintenanceService> maintenance = requires(MaintenanceService.class);
    Positive<Timer> timer = requires(Timer.class);
    // Instance
    private State state;
    private KeyRange responsibility;
    private Address self;
    private Map<Long, CaracalMsg> openOps = new TreeMap<Long, CaracalMsg>();
    private View view;

    // Stats
    private long storeSize = 0;
    private long storeNumberKeys = 0;
    private long numOps = 0;
    private long lastOpTS = 0;
    private UUID timerId = null;
    private long timerInterval;

    public MethCat(Meth init) {
        this.responsibility = init.responsibility;
        this.self = init.self;
        this.view = init.view;
        this.timerInterval = init.statsPeriod;

        state = State.FORWARDING;

        // subscriptions
        subscribe(forwardingHandler, network);
        subscribe(syncedHandler, replication);
    }
    Handler<Stop> stopHandler = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            if (timerId != null) {
                trigger(new CancelPeriodicTimeout(timerId), timer);
            }
        }
    };
    // FORWARDING
    Handler<CaracalMsg> forwardingHandler = new Handler<CaracalMsg>() {
        @Override
        public void handle(CaracalMsg event) {
            LOG.debug("{}: Received request {}. Cannot handle. Forwarding...", self, event);

            if (event.op instanceof RangeQuery.Request) {
                RangeQuery.Request req = (RangeQuery.Request) event.op;

                if (responsibility.contains(req.subRange)) {
                    // foward because we are not ready to actually handle requests ourself yet
                    forwardToViewMember(event);
                } else {
                    ForwardToRange ftr = new ForwardToRange(req, req.subRange, event.getSource());
                    trigger(ftr, lookup);
                }
            } else if (event.op instanceof GetRequest) {
                GetRequest req = (GetRequest) event.op;
                if (responsible(req.key)) {
                    // foward because we are not ready to actually handle requests ourself yet
                    forwardToViewMember(event);
                } else {
                    ForwardToAny fta = new ForwardToAny(req.key, event);
                    trigger(fta, lookup);
                }
            } else if (event.op instanceof PutRequest) {
                PutRequest req = (PutRequest) event.op;
                if (responsible(req.key)) {
                    // foward because we are not ready to actually handle requests ourself yet
                    forwardToViewMember(event);
                } else {
                    ForwardToAny fta = new ForwardToAny(req.key, event);
                    trigger(fta, lookup);
                }
            } else {
                LOG.warn("Unknown operation {}", event.op);
            }
        }
    };
    Handler<Synced> syncedHandler = new Handler<Synced>() {
        @Override
        public void handle(Synced event) {
            goToActive();
            trigger(new MaintenanceMsg(self, self, new NodeSynced(view, responsibility)), network);
        }
    };
    // ACTIVE
    Handler<CaracalMsg> requestHandler = new Handler<CaracalMsg>() {
        @Override
        public void handle(CaracalMsg event) {
            LOG.debug("{}: Received request {}. Processing...", self, event);

            if (event.op instanceof RangeQuery.Request) {
                RangeQuery.Request req = (RangeQuery.Request) event.op;
                if (responsibility.contains(req.subRange)) {
                    if (req.subRange.equals(KeyRange.EMPTY)) {
                        LOG.warn("Forwarding of ranges is defective, receiving empty range");
                        return;
                    }
                    openOps.put(event.op.id, event);
                    trigger(req, replication);
                } else {
                    ForwardToRange ftr = new ForwardToRange(req, req.subRange, event.getSource());
                    trigger(ftr, lookup);
                }
            } else if (event.op instanceof GetRequest) {
                GetRequest req = (GetRequest) event.op;
                if (responsible(req.key)) {
                    openOps.put(event.op.id, event);
                    trigger(event.op, replication);
                } else {
                    ForwardToAny fta = new ForwardToAny(req.key, event);
                    trigger(fta, lookup);
                }
            } else if (event.op instanceof PutRequest) {
                PutRequest req = (PutRequest) event.op;
                if (responsible(req.key)) {
                    openOps.put(event.op.id, event);
                    trigger(event.op, replication);
                } else {
                    ForwardToAny fta = new ForwardToAny(req.key, event);
                    trigger(fta, lookup);
                }
            } else {
                LOG.warn("Unknown operation {}", event.op);
            }
        }
    };

    Handler<CaracalResponse> responseHandler = new Handler<CaracalResponse>() {
        @Override
        public void handle(CaracalResponse event) {
            CaracalMsg orig = openOps.remove(event.id);
            if (orig == null) {
                LOG.debug("{}: Got {} but don't feel responsible for it.", self, event);
                // Message was either already answered or another node is responsible
                return;
            }
            if (event instanceof RangeQuery.Response) {
                RangeQuery.Request req = (RangeQuery.Request) orig.op;
                RangeQuery.Response resp = (RangeQuery.Response) event;
                resp.setReq(req);
                if (req.execType.equals(RangeQuery.Type.SEQUENTIAL) && !resp.readLimit) {
                    KeyRange endRange = KeyRange.closed(req.subRange.end).endFrom(req.initRange);

                    ForwardToRange fta = new ForwardToRange(req, endRange, orig.getSource());
                    trigger(fta, lookup);
                    LOG.debug("{}: RangeQuery sequentialy {} from {}", new Object[]{self, event, orig.getSource()});
                }
            }
            trigger(new CaracalMsg(self, orig.getSource(), event), network);
            LOG.debug("{}: Got {} and sent it back to {}", new Object[]{self, event, orig.getSource()});
        }
    };

    Handler<MaintenanceMsg> maintenanceHandler = new Handler<MaintenanceMsg>() {
        @Override
        public void handle(MaintenanceMsg event) {
            if (event.op instanceof Reconfiguration) {
                Reconfiguration reconf = (Reconfiguration) event.op;
                trigger(reconf.change, replication);
            }
        }
    };

    Handler<Diff> diffHandler = new Handler<Diff>() {

        @Override
        public void handle(Diff event) {
            if (event.reset) {
                storeSize = event.size;
                storeNumberKeys = event.keys;
            } else {
                storeSize += event.size;
                storeNumberKeys += event.keys;
            }
        }
    };

    Handler<StatsTimeout> timeoutHandler = new Handler<StatsTimeout>() {

        @Override
        public void handle(StatsTimeout event) {
            long time = System.currentTimeMillis();
            long ops = resetOpS(time);
            NodeStats stats = new NodeStats(self, responsibility, storeSize, storeNumberKeys, ops);
            trigger(stats, maintenance);
        }

    };

    private long resetOpS(long time) {
        long timediff = lastOpTS - time;
        double ops = Math.floor(((double) numOps) / ((double) timediff));
        lastOpTS = time;
        numOps = 0;
        return (long) ops;
    }

    private boolean responsible(Key k) {
        return responsibility.contains(k);
    }

    private void goToActive() {
        LOG.debug("{}: synced! Going active.", self);
        state = State.ACTIVE;
        // old subs
        unsubscribe(forwardingHandler, network);
        unsubscribe(syncedHandler, replication);
        // new subs
        subscribe(responseHandler, replication);
        subscribe(diffHandler, replication);
        subscribe(requestHandler, network);
        subscribe(maintenanceHandler, network);
        subscribe(timeoutHandler, timer);

        trigger(new ReplicationSetInfo(responsibility), replication);

        SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(timerInterval, timerInterval);
        StatsTimeout timeout = new StatsTimeout(spt);
        spt.setTimeoutEvent(timeout);
        trigger(spt, timer);
        timerId = timeout.getTimeoutId();
        lastOpTS = System.currentTimeMillis();
    }

    private void forwardToViewMember(CaracalMsg event) {
        for (Address adr : view.members) {
            if (!adr.equals(self)) {
                trigger(event.insertDestination(adr), network);
            }
        }
    }

    public static class StatsTimeout extends Timeout {

        StatsTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}
