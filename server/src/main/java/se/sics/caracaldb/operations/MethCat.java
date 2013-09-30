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
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class MethCat extends ComponentDefinition {

    public static enum State {

        FORWARDING,
        ACTIVE;
    }
    private static final Logger LOG = LoggerFactory.getLogger(MethCat.class);
    Positive<Network> net = requires(Network.class);
    Positive<Replication> rep = requires(Replication.class);
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
        subscribe(forwardingHandler, net);
        subscribe(syncedHandler, rep);
    }
    // FORWARDING
    Handler<CaracalMsg> forwardingHandler = new Handler<CaracalMsg>() {
        @Override
        public void handle(CaracalMsg event) {
            LOG.debug("{}: Got message {}. Need to forward somewhere...", self, event);
            Key k = targetKey(event.op);
            if (k == null) {
                LOG.warn("Couldn't find key for operation {}", event.op);
                return;
            }
            if (responsible(k)) {
                // foward because we are not ready to actually handle requests ourself yet
                forwardToViewMember(event);
            } else {
                ForwardToAny fta = new ForwardToAny(k, event);
                trigger(fta, lookup);
            }
        }
    };
    Handler<Synced> syncedHandler = new Handler<Synced>() {
        @Override
        public void handle(Synced event) {
            goToActive();
            trigger(new MaintenanceMsg(self, self, new NodeSynced(view, responsibility)), net);
        }
    };
    // ACTIVE
    Handler<CaracalMsg> requestHandler = new Handler<CaracalMsg>() {
        @Override
        public void handle(CaracalMsg event) {
            LOG.debug("{}: Got message {}. Trying to handle...", self, event);
            Key k = targetKey(event.op);
            if (k == null) {
                LOG.warn("Couldn't find key for operation {}", event.op);
                return;
            }
            if (responsible(k)) {
                openOps.put(event.op.id, event);
                trigger(event.op, rep);
            } else {
                ForwardToAny fta = new ForwardToAny(k, event);
                trigger(fta, lookup);
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
            trigger(new CaracalMsg(self, orig.getSource(), event), net);
            LOG.debug("{}: Got {} and sent it back to {}", new Object[] {self, event, orig.getSource()});
        }
    };
    Handler<MaintenanceMsg> maintenanceHandler = new Handler<MaintenanceMsg>() {
        @Override
        public void handle(MaintenanceMsg event) {
            if (event.op instanceof Reconfiguration) {
                Reconfiguration reconf = (Reconfiguration) event.op;
                trigger(reconf.change, rep);
            }
        }
    };

    private boolean responsible(Key k) {
        return responsibility.contains(k);
    }

    // TODO add code for ranges
    private Key targetKey(CaracalOp op) {
        if (op instanceof GetRequest) {
            GetRequest req = (GetRequest) op;
            return req.key;
        }
        if (op instanceof PutRequest) {
            PutRequest req = (PutRequest) op;
            return req.key;
        }

        return null;
    }

    private void goToActive() {
        LOG.debug("{}: synced! Going active.", self);
        state = State.ACTIVE;
        // old subs
        unsubscribe(forwardingHandler, net);
        unsubscribe(syncedHandler, rep);
        // new subs
        subscribe(responseHandler, rep);
        subscribe(requestHandler, net);
        subscribe(maintenanceHandler, net);
    }

    private void forwardToViewMember(CaracalMsg event) {
        for (Address adr : view.members) {
            if (!adr.equals(self)) {
                trigger(event.insertDestination(adr), net);
            }
        }
    }
}
