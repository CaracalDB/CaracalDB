/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication;

import com.google.common.primitives.Ints;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.View;
import se.sics.caracaldb.datatransfer.Completed;
import se.sics.caracaldb.datatransfer.DataReceiver;
import se.sics.caracaldb.datatransfer.DataReceiverInit;
import se.sics.caracaldb.datatransfer.DataSender;
import se.sics.caracaldb.datatransfer.DataSenderInit;
import se.sics.caracaldb.datatransfer.DataTransfer;
import se.sics.caracaldb.datatransfer.InitiateTransfer;
import se.sics.caracaldb.datatransfer.TransferFilter;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.PutResponse;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.paxos.Consensus;
import se.sics.caracaldb.paxos.Decide;
import se.sics.caracaldb.paxos.Propose;
import se.sics.caracaldb.paxos.Reconfigure;
import se.sics.caracaldb.store.GetReq;
import se.sics.caracaldb.store.GetResp;
import se.sics.caracaldb.store.Put;
import se.sics.caracaldb.store.Store;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.Stopped;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class PaxosSMR extends ComponentDefinition {

    public static enum State {

        PASSIVE, // wait for installation of first view
        SYNCING, // transfer data in the background and handle incoming requests
        READY; // handle requests
    }
    private static final Logger LOG = LoggerFactory.getLogger(PaxosSMR.class);
    Negative<Replication> rep = provides(Replication.class);
    Positive<Consensus> consensus = requires(Consensus.class);
    Positive<Store> store = requires(Store.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    // Instance
    private State state;
    private View view;
    private Address self;
    private PaxosSMRInit init;
    private Map<Class<? extends CaracalOp>, Action> actions = new HashMap<Class<? extends CaracalOp>, Action>();

    public PaxosSMR(PaxosSMRInit event) {
        this.init = event;
        
        this.view = init.view;
        this.self = init.self;

        provideOp(GetRequest.class, getAction);
        provideOp(PutRequest.class, putAction);

        
        state = State.PASSIVE;
        
        if (view == null) {
            LOG.debug("{}: Starting in passive mode", self);
            subscribe(installHandler, consensus);
        } else {
            LOG.debug("{}: Starting in active mode", self);
            subscribe(startHandler, control);
        }




    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            goToSyncing();
            goToReady();
        }
    };
    // Passive Handlers
    Handler<Reconfigure> installHandler = new Handler<Reconfigure>() {
        @Override
        public void handle(Reconfigure event) {
            unsubscribe(this, consensus);
            view = event.view;
            goToSyncing();

        }
    };
    // Syncing Handlers
    Handler<InitiateTransfer> transferHandler = new Handler<InitiateTransfer>() {
        @Override
        public void handle(InitiateTransfer event) {
            final Component dataTransfer = create(DataReceiver.class, new DataReceiverInit(event, false));
            connect(dataTransfer.getNegative(Network.class), net, new TransferFilter(event.id));
            connect(dataTransfer.getNegative(Timer.class), timer);
            connect(dataTransfer.getNegative(Store.class), store);

            final Handler<Completed> completedHandler = new Handler<Completed>() {
                @Override
                public void handle(Completed event) {
                    unsubscribe(this, dataTransfer.getPositive(DataTransfer.class));
                    trigger(Stop.event, dataTransfer.control());
                    Handler<Stopped> cleanupHandler = new Handler<Stopped>() {
                        @Override
                        public void handle(Stopped event) {
                            disconnect(dataTransfer.getNegative(Network.class), net);
                            disconnect(dataTransfer.getNegative(Timer.class), timer);
                            disconnect(dataTransfer.getNegative(Store.class), store);
                            destroy(dataTransfer);
                        }
                    };
                    subscribe(cleanupHandler, dataTransfer.control());

                    goToReady();
                }
            };
            subscribe(completedHandler, dataTransfer.getPositive(DataTransfer.class));
            trigger(Start.event, dataTransfer.control());
            unsubscribe(this, net); // Only allow one instance
        }
    };
    // In Group Handlers
    Handler<ViewChange> viewChangeHandler = new Handler<ViewChange>() {
        @Override
        public void handle(ViewChange event) {
            if ((view != null) && (view.id >= event.view.id)) {
                LOG.info("Ignoring view change from {} to {}: Local is at least as recent.",
                        view, event.view);
                return;
            }
            Reconfigure reconf = new Reconfigure(event.view, event.quorum);
            trigger(new Propose(0, reconf), consensus);
        }
    };
    Handler<Reconfigure> reconfHandler = new Handler<Reconfigure>() {
        @Override
        public void handle(Reconfigure event) {
            if ((view != null) && (view.id >= event.view.id)) {
                LOG.warn("Ignoring reconfiguration from {} to {}: Local is at least as recent.",
                        view, event.view);
                return;
            }

            View oldView = view;
            view = event.view;
            LOG.info("Moved to view {}", view);

            // transfer data
            transferDataMaybe(oldView, view);
        }
    };
    Handler<CaracalOp> opHandler = new Handler<CaracalOp>() {
        @Override
        public void handle(CaracalOp event) {
            if (!actions.containsKey(event.getClass())) {
                trigger(new CaracalResponse(event.id, ResponseCode.UNSUPPORTED_OP), rep);
                return;
            }
            trigger(new Propose(event.id, new SMROp(event)), consensus);
        }
    };
    Handler<SMROp> decideHandler = new Handler<SMROp>() {
        @Override
        public void handle(SMROp event) {
            LOG.debug("{}: Decided {}", self, event);
            Action a = actions.get(event.op.getClass());
            if (a == null) {
                LOG.error("No action for {}. This is weird...", event.op);
                return;
            }
            a.initiate(event.op);
        }
    };
    Handler<GetResp> getHandler = new Handler<GetResp>() {
        @Override
        public void handle(GetResp event) {
            trigger(new GetResponse(event.getId(), event.key, event.value), rep);
        }
    };
    // Actions
    Action<GetRequest> getAction = new Action<GetRequest>() {
        @Override
        public void initiate(GetRequest op) {
            GetReq request = new GetReq(op.key);
            request.setId(op.id);
            //LOG.debug("{}: requesting {}", self, request);
            trigger(request, store);
        }
    };
    Action<PutRequest> putAction = new Action<PutRequest>() {
        @Override
        public void initiate(PutRequest op) {
            Put request = new Put(op.key, op.data);
            trigger(request, store);
            //LOG.debug("{}: requesting {}", self, request);
            trigger(new PutResponse(op.id, op.key), rep);
        }
    };

    private <T extends CaracalOp> void provideOp(Class<T> type, Action<T> action) {
        actions.put(type, action);
    }

    private void goToSyncing() {
        if (state != State.PASSIVE) {
            LOG.error("Trying to go from {} to SYNCING!", state);
            return;
        }
        state = State.SYNCING;

        subscribe(transferHandler, net);

        subscribe(reconfHandler, consensus);
        subscribe(decideHandler, consensus);
        subscribe(viewChangeHandler, rep);
        subscribe(opHandler, rep);
        subscribe(getHandler, store);
    }

    private void goToReady() {
        if (state != State.SYNCING) {
            LOG.error("Trying to go from {} to READY!", state);
            return;
        }
        trigger(Synced.EVENT, rep);
    }

    private void transferDataMaybe(View oldView, View newView) {
        SortedSet<Address> responsible = new TreeSet<Address>();
        Address higher = oldView.members.ceiling(self);
        boolean last = higher.equals(self);
        Address lower = oldView.members.floor(self);
        boolean first = lower.equals(self);
        for (Address adr : newView.members) {
            if (!oldView.members.contains(adr) // it's new
                    && ((!first && (lower.compareTo(adr) < 0)) // it's between my predecessor (if exists)
                    && (self.compareTo(adr) > 0)) // and me
                    || (last // or I'm last in oldView
                    && (self.compareTo(adr) < 0))) { // and it's comes after me in newView
                responsible.add(adr);
            }
        }
        for (Address adr : responsible) {
            final Address dst = adr;
            long id = UUID.randomUUID().getLeastSignificantBits(); // TODO there's certainly better ways...
            final Component sender = create(DataSender.class, 
                    new DataSenderInit(id, init.range, self, dst, 
                    2*init.keepAlivePeriod, init.dataMessageSize));
            connect(sender.getNegative(Network.class), net, new TransferFilter(id));
            connect(sender.getNegative(Timer.class), timer);
            connect(sender.getNegative(Store.class), store);
            final Handler<Completed> completedHandler = new Handler<Completed>() {
                @Override
                public void handle(Completed event) {
                    LOG.info("{}: Completed transfer to {}", self, dst);
                    unsubscribe(this, sender.getPositive(DataTransfer.class));
                    trigger(Stop.event, sender.control());
                    Handler<Stopped> cleanupHandler = new Handler<Stopped>() {
                        @Override
                        public void handle(Stopped event) {
                            disconnect(sender.getNegative(Network.class), net);
                            disconnect(sender.getNegative(Timer.class), timer);
                            disconnect(sender.getNegative(Store.class), store);
                            destroy(sender);
                        }
                    };
                    subscribe(cleanupHandler, sender.control());
                }
            };
            subscribe(completedHandler, sender.getPositive(DataTransfer.class));
            trigger(Start.event, sender.control());
        }

    }

    public static class SMROp extends Decide {

        public final CaracalOp op;

        public SMROp(CaracalOp op) {
            this.op = op;
        }

        @Override
        public int compareTo(Decide o) {
            if (o instanceof SMROp) {
                SMROp that = (SMROp) o;
                if (this.op.id != that.op.id) {
                    long diff = this.op.id - that.op.id;
                    return Ints.saturatedCast(diff);
                }
                return 0;
            }
            return super.baseCompareTo(o);
        }

        @Override
        public String toString() {
            return "SMROp(" + op.toString() + ")";
        }
    }
}
