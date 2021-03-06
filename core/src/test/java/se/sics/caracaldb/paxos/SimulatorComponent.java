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
package se.sics.caracaldb.paxos;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.paxos.Commands.Fail;
import se.sics.caracaldb.paxos.Commands.Join;
import se.sics.caracaldb.paxos.Commands.Operation;
import se.sics.caracaldb.replication.log.Propose;
import se.sics.caracaldb.replication.log.Reconfigure;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Kompics;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.virtual.VirtualNetworkChannel;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.virtual.networkmodel.HostAddress;
import se.sics.kompics.virtual.simulator.MessageDestinationSelector;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class SimulatorComponent extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(SimulatorComponent.class);
    private static final Random RAND = new Random();
    Positive<PaxosExperiment> simulator = requires(PaxosExperiment.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    private TreeSet<Integer> portsInUse = new TreeSet<Integer>();
    private TreeMap<Integer, Component> components = new TreeMap<Integer, Component>();
    private TreeMap<Integer, Address> group = new TreeMap<Integer, Address>();
    private InetAddress localIP;
    private DecisionStore store;
    private UUID opId = new UUID(0, 0);

    public SimulatorComponent() {
        try {
            localIP = InetAddress.getLocalHost();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex.getMessage());
        }

        // Subscriptions

        subscribe(bootHandler, simulator);
        subscribe(verifyHandler, simulator);
        subscribe(terminateHandler, simulator);
        subscribe(opHandler, simulator);
        subscribe(joinHandler, simulator);
        subscribe(failHandler, simulator);
    }
    Handler<Commands.Start> bootHandler = new Handler<Commands.Start>() {
        @Override
        public void handle(Commands.Start event) {
            LOG.info("Starting {} nodes.", event.num);

            for (int i = 0; i < event.num; i++) {
                int port = getFreePort();
                group.put(port, new Address(localIP, port, null));
            }

            ImmutableSet<Address> readOnlyGroup = ImmutableSet.copyOf(group.values());

            store = new DecisionStore(readOnlyGroup);
            
            View v = new View(ImmutableSortedSet.copyOf(readOnlyGroup), 0);

            for (Address adr : group.values()) {
                bootNode(adr, v);
            }

        }
    };
    Handler<TerminateExperiment> terminateHandler = new Handler<TerminateExperiment>() {
        @Override
        public void handle(TerminateExperiment event) {
            LOG.info("Terminating Experiment.");
            Kompics.forceShutdown();
        }
    };
    Handler<Commands.Verify> verifyHandler = new Handler<Commands.Verify>() {
        @Override
        public void handle(Commands.Verify event) {
            PaxosTest.setStore(store);
            if (store.allDecided()) {
                LOG.info("Placing store for verification and terminating.");
                trigger(new TerminateExperiment(), simulator);
            }
        }
    };
    Handler<Commands.Operation> opHandler = new Handler<Commands.Operation>() {
        @Override
        public void handle(Operation event) {
            Entry<Integer, Component> e = components.firstEntry();
            Component c = e.getValue();
            Address adr = group.get(e.getKey());
            PaxosManager.PaxosOp op = new PaxosManager.PaxosOp(opId);
            trigger(new Propose(op), c.getNegative(PaxosManagerPort.class));
            store.proposed(adr, op);
            opId = new UUID(0, opId.getLeastSignificantBits()+1);
        }
    };
    Handler<Commands.Join> joinHandler = new Handler<Commands.Join>() {
        @Override
        public void handle(Join event) {
            Component oldC = components.firstEntry().getValue();
            int port = getFreePort();
            Address adr = new Address(localIP, port, null);
            group.put(port, adr);
            int epoch = store.join(adr);
            LOG.info("Node {} joining in epoch {} with new group {}", new Object[]{adr, epoch, group.toString()});
            //ImmutableSet<Address> emptyGroup = ImmutableSet.of();
            View v = new View(ImmutableSortedSet.copyOf(group.values()), epoch + 1);
            bootNode(adr, null);
            trigger(new Propose(new Reconfigure(UUID.randomUUID(), v, group.size() / 2 + 1, 0, KeyRange.EMPTY)), oldC.getNegative(PaxosManagerPort.class));

        }
    };
    Handler<Commands.Fail> failHandler = new Handler<Commands.Fail>() {
        @Override
        public void handle(Fail event) {
            LOG.info("Handing fail event.");
            //LOG.info(null);
            Component failC = null;
            // always let leader fail, since it's the only intereasting case
            int failP = group.firstKey();
//            int r = RAND.nextInt(components.size());
//            for (Integer port : components.keySet()) {
//                if (r == 0) {
//                    failP = port;
//                    break;
//                }
//                r--;
//            }
            failC = components.remove(failP);
            group.remove(failP);
            trigger(Stop.event, failC.control());
            disconnect(failC.getNegative(Network.class), net);
            disconnect(failC.getNegative(Timer.class), timer);
        }
    };

    private void bootNode(Address netSelf, View view) {

        Component deadLetterBox = create(VirtualNetworkChannel.DefaultDeadLetterComponent.class, Init.NONE);
        VirtualNetworkChannel vnc = VirtualNetworkChannel.connect(net,
                deadLetterBox.getNegative(Network.class),
                new MessageDestinationSelector(new HostAddress(netSelf)));
        Component manager = create(PaxosManager.class, new PaxosManagerInit(view, 100, netSelf, store));
        components.put(netSelf.getPort(), manager);

        connect(manager.getNegative(Network.class), net, new MessageDestinationSelector(new HostAddress(netSelf)));
        connect(manager.getNegative(Timer.class), timer);

        trigger(Start.event, deadLetterBox.control());
        trigger(Start.event, manager.control());
    }

    private int getFreePort() {
        // Since the ports are not actually opened there's no reason to check
        // whether or not they are actually free.
        // Only thing is that the simulation shouldn't reuse ports.
        if (portsInUse.isEmpty()) {
            int initPort = 49000;
            portsInUse.add(initPort);
            return initPort;
        }
        int p = portsInUse.last() + 1;
        portsInUse.add(p);
        return p;
    }
}
