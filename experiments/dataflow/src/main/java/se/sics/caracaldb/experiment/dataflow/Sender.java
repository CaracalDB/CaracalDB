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
import java.util.HashMap;
import java.util.Map;
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
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Kill;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.network.virtual.VirtualNetworkChannel;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;

/**
 *
 * @author lkroll
 */
public class Sender extends ComponentDefinition {

    static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
    // Components
    private final Component netC;
    private final Component flowC;
    private final Component timeC;
    // Ports
    Positive<Network> net = requires(Network.class);
    // Instance
    private final VirtualNetworkChannel vnc;

    public Sender() {
        Main.sender = this; // FIXME make this nice

        timeC = create(JavaTimer.class, Init.NONE);
        netC = create(NettyNetwork.class, new NettyInit(Main.self));
        connect(net.getPair(), netC.getPositive(Network.class));
        vnc = VirtualNetworkChannel.connect(net);
        flowC = create(FlowManager.class, new FlowManagerInit(Main.bufferSize, Main.minAlloc, Main.protocol, Main.self));
        vnc.addConnection(null, flowC.getNegative(Network.class));
        connect(flowC.getNegative(Timer.class), timeC.getPositive(Timer.class));

        // subscriptions
        subscribe(transferHandler, loopback);
    }

    Handler<StartTransfer> transferHandler = new Handler<StartTransfer>() {

        @Override
        public void handle(StartTransfer event) {
            HashCode hash = Main.getHash(event.f);
            final Component sourceC = create(FileTransferAdapter.class, 
                    new FileTransferAdapter.Init(event.f, Mode.SOURCE, hash));
            UUID id = UUID.randomUUID();
            Map<String, Object> metadata = new HashMap<String, Object>();
            metadata.put("filename", event.f.getName());
            metadata.put("filehash", hash);
            metadata.put("filesize", event.f.length());
            final DataSenderInit init = new DataSenderInit(id, Main.self, event.dst, Main.retryTime, metadata);
            final Component senderC = create(DataSender.class, init);
            connect(senderC.getNegative(Network.class), net);
            connect(senderC.getNegative(Timer.class), timeC.getPositive(Timer.class));
            connect(senderC.getNegative(DataFlow.class), flowC.getPositive(DataFlow.class));
            connect(senderC.getNegative(DataSource.class), sourceC.getPositive(DataSource.class));
            final Handler<Completed> cH = new Handler<Completed>() {

                @Override
                public void handle(Completed event) {
                    LOG.info("Data Transfer complete: {}", event.id);
                    trigger(Kill.event, senderC.control());
                    trigger(Kill.event, sourceC.control());
                    Main.completed();
                }
            };
            subscribe(cH, senderC.getPositive(DataTransfer.class));
            trigger(Start.event, sourceC.control());
            trigger(Start.event, senderC.control());
        }
    };

    void startTransfer(File f, Address dst) {
        trigger(new StartTransfer(f, dst), onSelf);
    }



    public static class StartTransfer implements KompicsEvent {

        public final File f;
        public final Address dst;

        private StartTransfer(File f, Address dst) {
            this.f = f;
            this.dst = dst;
        }
    }

}
