/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.fd;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import java.util.HashSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
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
public class SimpleEFD extends ComponentDefinition {
    
    private static final Logger LOG = LoggerFactory.getLogger(SimpleEFD.class);
    // PORTS
    Negative<EventualFailureDetector> fd = provides(EventualFailureDetector.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    // STATE
    private long period;
    private Address self;
    private HashSet<Address> liveSet = new HashSet<Address>();
    private LinkedListMultimap<Address, SubscribeNodeStatus> subscriptions =
            LinkedListMultimap.create();
    private HashSet<Address> activeSet = new HashSet<Address>();
    private HashSet<Address> lastActiveSet = new HashSet<Address>();
    private UUID timeoutId = null;
    private HashSet<Address> broadcastSet = new HashSet<Address>();

    public SimpleEFD(Init init) {
        this.period = init.timeout / 2l;
        this.self = init.self;

        // Subscriptions
        subscribe(startHandler, control);
        subscribe(stopHandler, control);
        subscribe(heartbeatHandler, net);
        subscribe(subHandler, fd);
        subscribe(unsubHandler, fd);
        subscribe(timeoutHandler, timer);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(period, period);
            EFDTimeout toe = new EFDTimeout(spt);
            spt.setTimeoutEvent(toe);
            timeoutId = toe.getTimeoutId();
            trigger(spt, timer);
            broadcast();
        }
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            liveSet.clear();
            subscriptions.clear();
            activeSet.clear();
            lastActiveSet.clear();
            broadcastSet.clear();
            if (timeoutId != null) {
                trigger(new CancelPeriodicTimeout(timeoutId), timer);
                timeoutId = null;
            }
            LOG.debug("{}: Stopping", self);
        }
    };
    Handler<Heartbeat> heartbeatHandler = new Handler<Heartbeat>() {

        @Override
        public void handle(Heartbeat event) {
            if (broadcastSet.add(event.getSource())) {
                trigger(new Heartbeat(self, event.getSource()), net); // reply immediately
            }
            activeSet.add(event.getSource());
        }
    };
    Handler<SubscribeNodeStatus> subHandler = new Handler<SubscribeNodeStatus>() {

        @Override
        public void handle(SubscribeNodeStatus event) {
            subscriptions.put(event.node, event);
            broadcastSet.add(event.node);
            liveSet.add(event.node);
            // In case there is a timeout event directly after the subscription
            lastActiveSet.add(event.node);
            trigger(new Heartbeat(self, event.node), net);
        }
    };
    Handler<UnsubscribeNodeStatus> unsubHandler = new Handler<UnsubscribeNodeStatus>() {

        @Override
        public void handle(UnsubscribeNodeStatus event) {
            SubscribeNodeStatus r = null;
            for (SubscribeNodeStatus req : subscriptions.get(event.node)) {
                if (req.requestId.equals(event.requestId)) {
                    r = req;
                    break;
                }
            }
            if (r == null) {
                return;
            }
            subscriptions.remove(r.node, r);
//            if (subscriptions.containsKey(r.node)) {
//                return;
//            }
        }
        
    };
    Handler<EFDTimeout> timeoutHandler = new Handler<EFDTimeout>() {

        @Override
        public void handle(EFDTimeout event) {
//            LOG.debug("{}: FD timeout. \n   Subs: {} \n     Live: {}", 
//                    new Object[] {self, subscriptions.keySet(), liveSet});
            SetView<Address> allActive = Sets.union(activeSet, lastActiveSet);
            SetView<Address> subbedLive = Sets.intersection(liveSet, subscriptions.keySet());
            SetView<Address> subbedActive = Sets.intersection(allActive, subscriptions.keySet());
            SetView<Address> failed = Sets.difference(subbedLive, subbedActive);
            SetView<Address> restored = Sets.difference(subbedActive, subbedLive);
            for (Address adr : failed) {
                for (SubscribeNodeStatus req : subscriptions.get(adr)) {
                    trigger(new Suspect(req, adr), fd);
                }
            }
            //LOG.debug("{}: Failed: {}", self, failed);
            for (Address adr : restored) {
                for (SubscribeNodeStatus req : subscriptions.get(adr)) {
                    trigger(new Restore(req, adr), fd);
                }
            }
            //LOG.debug("{}: Restored: {}", self, restored);
            lastActiveSet = activeSet;
            activeSet = new HashSet<Address>();
            liveSet.clear();
            liveSet.addAll(subbedActive);
            broadcast();
        }
    };

    private void broadcast() {
        for (Address adr : broadcastSet) {
            trigger(new Heartbeat(self, adr), net);
        }
    }

    public static class Heartbeat extends Message {

        public Heartbeat(Address source, Address dest) {
            super(source, dest);
        }
    }

    public static class Init extends se.sics.kompics.Init<SimpleEFD> {

        public final long timeout;
        public final Address self;

        public Init(long timeout, Address self) {
            this.timeout = timeout;
            this.self = self;
        }
    }

    public static class EFDTimeout extends Timeout {

        public EFDTimeout(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}
