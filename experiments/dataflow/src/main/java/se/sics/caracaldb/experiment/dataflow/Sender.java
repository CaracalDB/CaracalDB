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
package se.sics.caracaldb.experiment.dataflow;

import com.google.common.hash.HashCode;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.datatransfer.Completed;
import se.sics.caracaldb.datatransfer.DataSender;
import se.sics.caracaldb.datatransfer.DataSenderInit;
import se.sics.caracaldb.datatransfer.DataSource;
import se.sics.caracaldb.datatransfer.DataTransfer;
import se.sics.caracaldb.experiment.dataflow.FileTransferAdapter.Mode;
import se.sics.caracaldb.flow.DataFlow;
import se.sics.caracaldb.flow.FlowManager;
import se.sics.caracaldb.flow.FlowManagerInit;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.network.data.DataNetwork;
import se.sics.kompics.network.data.DataNetwork.NetHook;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.network.virtual.VirtualNetworkChannel;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;

/**
 *
 * @author lkroll
 */
public class Sender extends ComponentDefinition {

    static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
    static final long PINGTIME = 100;
    // Components
    private final Component netC;
    private final Component flowC;
    private final Component timeC;
    // Ports
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    // Instance
    private final VirtualNetworkChannel vnc;
    private final Address self;
    private final Map<UUID, SettableFuture<StatsWithRTTs>> promises = new HashMap<UUID, SettableFuture<StatsWithRTTs>>();
    private final Transport pingProtocol;
    private final Map<UUID, Address> pingees = new HashMap<UUID, Address>();
    private final Map<UUID, Integer> pingNum = new HashMap<UUID, Integer>();
    private final Map<UUID, ArrayList<Long>> pongstats = new HashMap<UUID, ArrayList<Long>>();
    private UUID timeoutId;

    public Sender() {
        this(new Init(Main.self, Main.bufferSize, Main.minAlloc, Main.maxAlloc, Main.protocol, null));
    }

    public Sender(Init init) {
        Main.sender = this; // FIXME make this nice

        self = init.self;
        pingProtocol = init.pingProtocol;

        timeC = create(JavaTimer.class, Init.NONE);
        connect(timer.getPair(), timeC.getPositive(Timer.class));
        netC = create(DataNetwork.class, new DataNetwork.Init(new NetHook() {

            @Override
            public Component setupNetwork(ComponentProxy proxy) {
                Component nettyC = create(NettyNetwork.class, new NettyInit(self));
                return nettyC;
            }

            @Override
            public void connectTimer(ComponentProxy proxy, Component c) {
                proxy.connect(timeC.getPositive(Timer.class), c.getNegative(Timer.class), Channel.TWO_WAY);
            }
        }));
        connect(net.getPair(), netC.getPositive(Network.class));
        vnc = VirtualNetworkChannel.connect(net, proxy);
        flowC = create(FlowManager.class, new FlowManagerInit(init.bufferSize, init.minAlloc, init.maxAlloc, Transport.TCP, init.protocol, init.self));
        vnc.addConnection(null, flowC.getNegative(Network.class));
        connect(flowC.getNegative(Timer.class), timeC.getPositive(Timer.class));

        // subscriptions
        subscribe(startHandler, control);
        subscribe(stopHandler, control);
        subscribe(pingHandler, timer);
        subscribe(pongHandler, net);
        subscribe(pingstartHandler, loopback);
        subscribe(transferHandler, loopback);
        subscribe(statsHandler, net);
    }

    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            if (pingProtocol != null) {
                SchedulePeriodicTimeout spt
                        = new SchedulePeriodicTimeout(0, PINGTIME);
                PingTimeout timeoutEvent = new PingTimeout(spt);
                spt.setTimeoutEvent(timeoutEvent);
                timeoutId = timeoutEvent.getTimeoutId();
                trigger(spt, timer);
            }
        }
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            if (timeoutId != null) {
                trigger(new CancelPeriodicTimeout(timeoutId), timer);
            }
        }

    };
    Handler<PingTimeout> pingHandler = new Handler<PingTimeout>() {

        @Override
        public void handle(PingTimeout event) {
            long ts = System.currentTimeMillis();
            for (Entry<UUID, Address> e : pingees.entrySet()) {
                trigger(new Control.Ping(self, e.getValue(), pingProtocol, e.getKey(), ts), net);
            }
        }
    };
    Handler<Control.Pong> pongHandler = new Handler<Control.Pong>() {

        @Override
        public void handle(Control.Pong event) {
            long ts = System.currentTimeMillis();
            long diff = ts - event.ts;
            ArrayList<Long> pstats = pongstats.get(event.id);
            pstats.add(diff);
            Integer num = pingNum.get(event.id);
            if ((num != null) && (num <= pstats.size())) { // enough pings
                pingNum.remove(event.id);
                pingees.remove(event.id);
                pongstats.remove(event.id);
                SettableFuture<StatsWithRTTs> promise = promises.remove(event.id);

                if (promises == null) {
                    LOG.error("Got pings for a transfer that never started: {} -> {}", event.id, pstats);
                } else {
                    promise.set(new StatsWithRTTs(null, pstats));
                }
            }
        }
    };

    Handler<StartTransfer> transferHandler = new Handler<StartTransfer>() {

        @Override
        public void handle(StartTransfer event) {
            HashCode hash = Main.getHash(event.f);
            final Component sourceC = create(FileTransferAdapter.class,
                    new FileTransferAdapter.Init(event.f, Mode.SOURCE, hash));
            final UUID id = UUID.randomUUID();
            promises.put(id, event.promise);
            pingees.put(id, event.dst);
            pongstats.put(id, new ArrayList<Long>());
            Map<String, Object> metadata = new HashMap<String, Object>();
            metadata.put("filename", event.f.getName());
            metadata.put("filehash", hash);
            metadata.put("filesize", event.f.length());
            final DataSenderInit init = new DataSenderInit(id, self, event.dst, Main.retryTime, metadata);
            final Component senderC = create(DataSender.class, init);
            connect(senderC.getNegative(Network.class), net);
            connect(senderC.getNegative(Timer.class), timeC.getPositive(Timer.class));
            connect(senderC.getNegative(DataFlow.class), flowC.getPositive(DataFlow.class));
            connect(senderC.getNegative(DataSource.class), sourceC.getPositive(DataSource.class));
            final Handler<Completed> cH = new Handler<Completed>() {

                @Override
                public void handle(Completed event) {
                    pingees.remove(id);
                    LOG.info("Data Transfer complete: {}", event.id);
                    trigger(Kill.event, senderC.control());
                    trigger(Kill.event, sourceC.control());
                }
            };
            subscribe(cH, senderC.getPositive(DataTransfer.class));
            trigger(Start.event, sourceC.control());
            trigger(Start.event, senderC.control());
        }
    };
    Handler<StartPings> pingstartHandler = new Handler<StartPings>() {

        @Override
        public void handle(StartPings event) {
            final UUID id = UUID.randomUUID();
            promises.put(id, event.promise);
            pingees.put(id, event.dst);
            pongstats.put(id, new ArrayList<Long>());
            pingNum.put(id, event.num);
        }
    };
    Handler<TSMessage> statsHandler = new Handler<TSMessage>() {

        @Override
        public void handle(TSMessage event) {
            SettableFuture<StatsWithRTTs> promise = promises.remove(event.id);

            ArrayList<Long> pstats = pongstats.remove(event.id);
            if (promises == null) {
                LOG.error("Got stats for a transfer that never started: {} -> {}", event.id, event.stats);
            } else {
                promise.set(new StatsWithRTTs(event.stats, pstats));
            }
        }
    };

    public ListenableFuture<StatsWithRTTs> startTransfer(File f, Address dst) {
        StartTransfer st = new StartTransfer(f, dst);
        trigger(st, onSelf);
        return st.promise;
    }

    public ListenableFuture<StatsWithRTTs> startPingOnly(Address dst, int num) {
        StartPings sp = new StartPings(dst, num);
        trigger(sp, onSelf);
        return sp.promise;
    }

    public static class StartTransfer implements KompicsEvent {

        public final File f;
        public final Address dst;
        public final SettableFuture<StatsWithRTTs> promise = SettableFuture.create();

        private StartTransfer(File f, Address dst) {
            this.f = f;
            this.dst = dst;
        }
    }

    public static class StartPings implements KompicsEvent {

        public final Address dst;
        public final int num;
        public final SettableFuture<StatsWithRTTs> promise = SettableFuture.create();

        private StartPings(Address dst, int num) {
            this.dst = dst;
            this.num = num;
        }
    }

    public static class PingTimeout extends Timeout {

        public PingTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }

    }

    public static final class Init extends se.sics.kompics.Init<Sender> {

        public final Address self;
        public final long bufferSize;
        public final long minAlloc;
        public final long maxAlloc;
        public final Transport protocol;
        public final Transport pingProtocol;

        public Init(Address self, long bufferSize, long minAlloc, long maxAlloc, Transport protocol, Transport pingProtocol) {
            this.self = self;
            this.bufferSize = bufferSize;
            this.minAlloc = minAlloc;
            this.maxAlloc = maxAlloc;
            this.protocol = protocol;
            this.pingProtocol = pingProtocol;
        }
    }
}
