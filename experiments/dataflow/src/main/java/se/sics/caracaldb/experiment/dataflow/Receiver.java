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
import java.io.File;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.datatransfer.Completed;
import se.sics.caracaldb.datatransfer.DataReceiver;
import se.sics.caracaldb.datatransfer.DataReceiverInit;
import se.sics.caracaldb.datatransfer.DataSink;
import se.sics.caracaldb.datatransfer.DataTransfer;
import se.sics.caracaldb.datatransfer.InitiateTransfer;
import se.sics.caracaldb.flow.DataFlow;
import se.sics.caracaldb.flow.FlowManager;
import se.sics.caracaldb.flow.FlowManagerInit;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kill;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.network.virtual.VirtualNetworkChannel;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;

/**
 *
 * @author lkroll
 */
public class Receiver extends ComponentDefinition {

    static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
    // Components
    private final Component netC;
    private final Component flowC;
    private final Component timeC;
    // Ports
    Positive<Network> net = requires(Network.class);
    // Instance
    private final VirtualNetworkChannel vnc;
    private final BlockingQueue<TransferStats> resultQ;
    private final Address self;
    private final File outputDirectory;

    public Receiver() {
        this(new Init(Main.self, Main.bufferSize, Main.minAlloc, Main.maxAlloc, Main.protocol, Main.resultQ, new File(".")));
    }

    public Receiver(Init init) {
        resultQ = init.resultQ;
        self = init.self;
        outputDirectory = init.outputDirectory;

        timeC = create(JavaTimer.class, Init.NONE);
        netC = create(NettyNetwork.class, new NettyInit(init.self));
        connect(net.getPair(), netC.getPositive(Network.class));
        vnc = VirtualNetworkChannel.connect(net, proxy);
        flowC = create(FlowManager.class, new FlowManagerInit(init.bufferSize, init.minAlloc, init.maxAlloc, Transport.TCP, init.protocol, init.self));
        vnc.addConnection(null, flowC.getNegative(Network.class));
        connect(flowC.getNegative(Timer.class), timeC.getPositive(Timer.class));

        // subscriptions
        subscribe(initHandler, net);
        subscribe(startHandler, control);
        subscribe(pingHandler, net);
    }

    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            if (!outputDirectory.exists()) {
                outputDirectory.mkdirs();
            }
            if (!outputDirectory.isDirectory()) {
                LOG.error("Output directory is not actually a directory!");
                throw new RuntimeException("Invalid output directory.");
            }
            LOG.info("{}: Waitinf for incoming data transfer.", self);
        }
    };

    Handler<Control.Ping> pingHandler = new Handler<Control.Ping>(){

        @Override
        public void handle(Control.Ping event) {
            trigger(event.reply(), net);
        }
    };
    
    Handler<InitiateTransfer> initHandler = new Handler<InitiateTransfer>() {

        @Override
        public void handle(InitiateTransfer event) {
            LOG.info("{}: Initiating transfer: {}", self, event);
            final Address sender = event.getSource();
            String filename = (String) event.metadata.get("filename");
            final File f = outputDirectory.toPath().resolve(filename).toFile();
            final HashCode hash = (HashCode) event.metadata.get("filehash");
            final long filesize = (Long) event.metadata.get("filesize");
            final Component sinkC = create(FileTransferAdapter.class,
                    new FileTransferAdapter.Init(f, FileTransferAdapter.Mode.SINK, hash));
            final Component drecv = create(DataReceiver.class, new DataReceiverInit(event));
            connect(drecv.getNegative(Network.class), net);
            connect(drecv.getNegative(Timer.class), timeC.getPositive(Timer.class));
            connect(drecv.getNegative(DataFlow.class), flowC.getPositive(DataFlow.class));
            connect(drecv.getNegative(DataSink.class), sinkC.getPositive(DataSink.class));
            final long startt = System.currentTimeMillis();
            final Handler<Completed> cH = new Handler<Completed>() {

                @Override
                public void handle(Completed event) {
                    long endt = System.currentTimeMillis();
                    long difft = endt - startt;
                    double diffs = (double) difft / 1000.0;
                    double fsizekb = (double) filesize / 1024.0;
                    double avg = fsizekb / diffs; // in kb/s
                    TransferStats stats = new TransferStats(filesize, difft);
                    trigger(new TSMessage(self, sender, stats, event.id), net);
                    LOG.info("Data Transfer {} completed. {}kb in {}s (avg. throughput {}kb/s)", new Object[]{event.id, fsizekb, diffs, avg});
                    trigger(Kill.event, drecv.control());
                    trigger(Kill.event, sinkC.control());
                    try {
                        resultQ.put(stats);
                    } catch (InterruptedException ex) {
                        LOG.error("Failed to notify of transfer completion: {}", ex);
                    }
                }
            };
            subscribe(cH, drecv.getPositive(DataTransfer.class));
            trigger(Start.event, sinkC.control());
            trigger(Start.event, drecv.control());
        }
    };

    public static final class Init extends se.sics.kompics.Init<Receiver> {

        public final Address self;
        public final long bufferSize;
        public final long minAlloc;
        public final long maxAlloc;
        public final Transport protocol;
        public final BlockingQueue<TransferStats> resultQ;
        public final File outputDirectory;

        public Init(Address self, long bufferSize, long minAlloc, long maxAlloc, Transport protocol, BlockingQueue<TransferStats> resultQ, File outputDirectory) {
            this.self = self;
            this.bufferSize = bufferSize;
            this.minAlloc = minAlloc;
            this.maxAlloc = maxAlloc;
            this.protocol = protocol;
            this.resultQ = resultQ;
            this.outputDirectory = outputDirectory;
        }
    }

}
