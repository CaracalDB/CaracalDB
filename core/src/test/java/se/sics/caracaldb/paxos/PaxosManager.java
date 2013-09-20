/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos;

import com.google.common.primitives.Ints;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.View;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.fd.SimpleEFD;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class PaxosManager extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(PaxosManager.class);
    Positive<PaxosManagerPort> pm = requires(PaxosManagerPort.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    Positive<EventualFailureDetector> fd = requires(EventualFailureDetector.class);
    Positive<Consensus> consensus = requires(Consensus.class);
    Component paxos;
    Component fdComp;
    private Address self;
    private DecisionStore store;
    private View view;

    public PaxosManager(PaxosManagerInit init) {

        self = init.self;
        store = init.store;
        view = init.view;

        int quorum = 0;
        if (view != null) {
            quorum = view.members.size() / 2 + 1;
        }
        fdComp = create(SimpleEFD.class, new SimpleEFD.Init(init.networkBound, self));
        paxos = create(Paxos.class, new PaxosInit(view, quorum, init.networkBound, self));

        connect(paxos.getPositive(Consensus.class), consensus.getPair());
        connect(fdComp.getPositive(EventualFailureDetector.class), fd.getPair());
        // neg
        connect(fdComp.getNegative(Network.class), net);
        connect(fdComp.getNegative(Timer.class), timer);
        connect(paxos.getNegative(Network.class), net);
        //connect(paxos.getNegative(Timer.class), timer);
        connect(paxos.getNegative(EventualFailureDetector.class), fd);

        // subscriptions
        subscribe(proposeHandler, pm);
        subscribe(decideHandler, consensus);
        subscribe(reconfigHandler, consensus);
        subscribe(stopHandler, control);
    }
    Handler<Propose> proposeHandler = new Handler<Propose>() {
        @Override
        public void handle(Propose event) {
            LOG.debug("{}: Got Propose({})", self, event.id);
            trigger(event, consensus);
        }
    };
    Handler<PaxosOp> decideHandler = new Handler<PaxosOp>() {
        @Override
        public void handle(PaxosOp event) {
            LOG.debug("{}: Got Decide({}) in epoch {}", new Object[]{self, event.id, view.id});
            store.decided(view.id, self, event.id);
        }
    };
    Handler<Reconfigure> reconfigHandler = new Handler<Reconfigure>() {
        @Override
        public void handle(Reconfigure event) {
            if (view == null) {
                store.joined(self);
            }
            view = event.view;
            LOG.debug("{}: Got Reconfigure, going to epoch {}", self, view.id);
        }
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            LOG.debug("{}: Failed in epoch {}", self, view.id);
            store.fail(view.id, self);
        }
    };
    
    @Override
    public void tearDown() {
        disconnect(paxos.getPositive(Consensus.class), consensus.getPair());
        disconnect(fdComp.getPositive(EventualFailureDetector.class), fd.getPair());
        // neg
        disconnect(fdComp.getNegative(Network.class), net);
        disconnect(fdComp.getNegative(Timer.class), timer);
        disconnect(paxos.getNegative(Network.class), net);
        //connect(paxos.getNegative(Timer.class), timer);
        disconnect(paxos.getNegative(EventualFailureDetector.class), fd);
        
        destroy(fdComp);
        destroy(paxos);
    }

    public static class PaxosOp extends Decide {

        public long id;

        public PaxosOp(long id) {
            this.id = id;
        }

        @Override
        public int compareTo(Decide o) {
            if (o instanceof PaxosOp) {
                PaxosOp pop = (PaxosOp) o;
                return Ints.checkedCast(id - pop.id);
            }
            return super.baseCompareTo(o);
        }

        @Override
        public String toString() {
            return "PaxosOp(" + id + ")";
        }
    }
}
