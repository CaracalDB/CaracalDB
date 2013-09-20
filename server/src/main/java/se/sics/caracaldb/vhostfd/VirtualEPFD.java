/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.vhostfd;

import se.sics.caracaldb.fd.SubscribeNodeStatus;
import se.sics.caracaldb.fd.Restore;
import se.sics.caracaldb.fd.UnsubscribeNodeStatus;
import se.sics.caracaldb.fd.Suspect;
import se.sics.caracaldb.fd.EventualFailureDetector;
import com.google.common.base.Objects;
import com.google.common.collect.LinkedListMultimap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Request;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.timer.CancelTimeout;
import se.sics.kompics.timer.ScheduleTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class VirtualEPFD extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(VirtualEPFD.class);
    private static final Transport PROTOCOL = Transport.TCP;
    // PORTS
    Negative<EventualFailureDetector> fd = provides(EventualFailureDetector.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    // STATE
    private final HashSet<UUID> outstandingTimeouts = new HashSet<UUID>();
    private HashMap<Address, HostProber> hostProbers = new HashMap<Address, HostProber>();
    long minRto, livePingInterval, deadPingInterval, pongTimeoutIncrement;
    private Address self;

    public VirtualEPFD(VEPFDInit init) {
        this.minRto = init.minRto;
        this.livePingInterval = init.livePingInterval;
        this.deadPingInterval = init.deadPingInterval;
        this.pongTimeoutIncrement = init.pongTimeoutIncrement;
        this.self = init.self;

        // Subscriptions
        subscribe(subscribeHandler, fd);
        subscribe(unsubscribeHandler, fd);
        subscribe(pingTimeoutHandler, timer);
        subscribe(pongTimeoutHandler, timer);
        subscribe(pingHandler, net);
        subscribe(pongHandler, net);
    }
    Handler<SubscribeNodeStatus> subscribeHandler = new Handler<SubscribeNodeStatus>() {
        @Override
        public void handle(SubscribeNodeStatus event) {
            Address hostAddress = event.node.hostAddress();
            HostProber hostProber = hostProbers.get(hostAddress);
            if (hostProber == null) {
                hostProber = new HostProber(hostAddress, VirtualEPFD.this);
                hostProber.addRequest(event);
                hostProbers.put(hostAddress, hostProber);
                hostProber.start();
                LOG.debug("@{}: Started probing host {}", self, hostAddress);
            } else {
                hostProber.addRequest(event);
                LOG.debug("@{}: Host {} is already being probed", self, hostAddress);
            }
        }
    };
    Handler<UnsubscribeNodeStatus> unsubscribeHandler = new Handler<UnsubscribeNodeStatus>() {
        @Override
        public void handle(UnsubscribeNodeStatus event) {
            Address hostAddress = event.node.hostAddress();
            HostProber prober = hostProbers.get(hostAddress);
            if (prober != null) {
                UUID requestId = event.requestId;
                if (prober.hasRequest(requestId)) {
                    boolean last = prober.removeRequest(requestId);
                    if (last) {
                        hostProbers.remove(hostAddress);
                        prober.stop();
                        LOG.debug("@{}: Stopped probing host {}",
                                self, hostAddress);
                    }
                } else {
                    LOG.warn("@{}: I have no request of id {} for the probing of host {}",
                            new Object[]{self, requestId, hostAddress});
                }
            } else {
                LOG.debug("@{}: Host {} is not currently being probed (STOP)",
                        self.getId(), hostAddress);
            }
        }
    };
    Handler<SendPing> pingTimeoutHandler = new Handler<SendPing>() {
        @Override
        public void handle(SendPing event) {
            Address host = event.getHost();
            HostProber prober = hostProbers.get(host);
            if (prober != null) {
                prober.ping();
            } else {
                LOG.debug(
                        "@{}: Host {} is not currently being probed (SEND_PING)",
                        self, host);
            }
        }
    };
    Handler<PongTimedOut> pongTimeoutHandler = new Handler<PongTimedOut>() {
        @Override
        public void handle(PongTimedOut event) {
            if (outstandingTimeouts.contains(event.getTimeoutId())) {
                Address host = event.getHost();
                HostProber hostProber = hostProbers.get(host);
                outstandingTimeouts.remove(event.getTimeoutId());
                if (hostProber != null) {
                    LOG.debug("@{}: {} SUSPECTED DUE TO TIMEOUT {}",
                            new Object[]{self, host, event.getTimeoutId()});

                    hostProber.pongTimedOut();
                } else {
                    LOG.debug("@{}: Host {} is not currently being probed (TIMEOUT)",
                            self, host);
                }
            }
        }
    };
    Handler<Ping> pingHandler = new Handler<Ping>() {
        @Override
        public void handle(Ping event) {
            LOG.debug("@{}: Received Ping from {}. Sending Pong. ",
                    self, event.getSource());
            trigger(new Pong(event.getId(), event.getTs(), self,
                    event.getSource(), PROTOCOL), net);
        }
    };
    Handler<Pong> pongHandler = new Handler<Pong>() {
        @Override
        public void handle(Pong event) {
            // logger.debug("@{}: Received Pong({}, {}) from {}. ",
            // new Object[] { self.getId(), event.getTs(), event.getId(),
            // event.getSource() });
            if (outstandingTimeouts.remove(event.id)) {
                LOG.debug("Canceled timer id {}", event.id);
                trigger(new CancelTimeout(event.id), timer);
            }
            Address host = event.getSource();
            HostProber hostProber = hostProbers.get(host);

            if (hostProber != null) {
                hostProber.pong(event.id, event.ts);
            } else {
                LOG.debug("@{}: Host {} is not currently being probed (GOT_PONG)",
                        self, host);
            }
        }
    };

    UUID setPingTimer(boolean suspected, Address probedHost) {
        long interval = suspected ? deadPingInterval : livePingInterval;
        ScheduleTimeout st = new ScheduleTimeout(interval);
        st.setTimeoutEvent(new SendPing(st, probedHost));
        UUID intervalPingTimeoutId = st.getTimeoutEvent().getTimeoutId();

        // we must not add this timeout id to outstandingTimeout!

        trigger(st, timer);
        return intervalPingTimeoutId;
    }

    UUID sendPing(long ts, Address probedHost, long expectedRtt) {
        // Setting timer for the receiving the Pong packet
        ScheduleTimeout st = new ScheduleTimeout(expectedRtt
                + pongTimeoutIncrement);
        st.setTimeoutEvent(new PongTimedOut(st, probedHost));
        UUID pongTimeoutId = st.getTimeoutEvent().getTimeoutId();
        outstandingTimeouts.add(pongTimeoutId);

        trigger(st, timer);
        trigger(new Ping(pongTimeoutId, ts, self, probedHost, PROTOCOL), net);
        // logger.debug(
        // "@{}: Sent Ping({},{},{}) to {}.",
        // new Object[] { self.getId(), ts, pongTimeoutId,
        // pongTimeoutId.hashCode(), probedPeer });
        return pongTimeoutId;
    }

    void suspect(Suspect suspectEvent) {
        trigger(suspectEvent, fd);
        LOG.debug("Node {} is suspected", suspectEvent.node);
    }

    void restore(Restore restoreEvent) {
        trigger(restoreEvent, fd);
        LOG.debug("Node {} is restored", restoreEvent.node);
    }

    void stop(UUID intervalPingTimeoutId, UUID pongTimeoutId) {
        if (outstandingTimeouts.remove(intervalPingTimeoutId)) {
            trigger(new CancelTimeout(intervalPingTimeoutId), timer);
        }
        if (outstandingTimeouts.remove(pongTimeoutId)) {
            trigger(new CancelTimeout(pongTimeoutId), timer);
        }
    }

    public final class SendPing extends Timeout {

        private final Address host;

        public SendPing(ScheduleTimeout request, Address host) {
            super(request);
            this.host = host;
        }

        public final Address getHost() {
            return host;
        }
    }

    public final class PongTimedOut extends Timeout {

        private final Address host;

        public PongTimedOut(ScheduleTimeout request, Address host) {
            super(request);
            this.host = host;
        }

        public final Address getHost() {
            return host;
        }
    }

    public final class Ping extends Message {

        /**
         *
         */
        private static final long serialVersionUID = 7927599827379778378L;
        private final UUID id;
        private final long ts;

        public Ping(UUID id, long ts, Address source, Address destination,
                Transport protocol) {
            super(source, destination, protocol);
            this.id = id;
            this.ts = ts;
        }

        public final UUID getId() {
            return id;
        }

        public final long getTs() {
            return ts;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result + (int) (ts ^ (ts >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Ping) {
                Ping that = (Ping) obj;
                if (!Objects.equal(this.id, that.id)) {
                    return false;
                }
                return (this.ts == that.ts);
            }
            return false;
        }
    }

    public final class Pong extends Message {

        /**
         *
         */
        private static final long serialVersionUID = 554912880067612945L;
        public final UUID id;
        public final long ts;

        public Pong(UUID id, long ts, Address source, Address destination,
                Transport protocol) {
            super(source, destination, protocol);
            this.id = id;
            this.ts = ts;
        }
    }
}
