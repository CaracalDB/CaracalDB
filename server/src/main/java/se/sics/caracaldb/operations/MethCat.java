/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.operations;

import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.global.ForwardToAny;
import se.sics.caracaldb.global.ForwardToRange;
import se.sics.caracaldb.global.LookupService;
import se.sics.caracaldb.global.MaintenanceMsg;
import se.sics.caracaldb.global.NodeSynced;
import se.sics.caracaldb.global.Reconfiguration;
import se.sics.caracaldb.replication.linearisable.Replication;
import se.sics.caracaldb.replication.linearisable.Synced;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;

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
    // Instance
    private State state;
    private KeyRange responsibility;
    private Address self;
    private Map<Long, CaracalMsg> openOps = new TreeMap<Long, CaracalMsg>();
    private View view;

    public MethCat(Meth init) {
        this.responsibility = init.responsibility;
        this.self = init.self;
        this.view = init.view;

        state = State.FORWARDING;

        // subscriptions
        subscribe(forwardingHandler, network);
        subscribe(syncedHandler, replication);
    }
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
        subscribe(requestHandler, network);
        subscribe(maintenanceHandler, network);
    }

    private void forwardToViewMember(CaracalMsg event) {
        for (Address adr : view.members) {
            if (!adr.equals(self)) {
                trigger(event.insertDestination(adr), network);
            }
        }
    }
}