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
package se.sics.caracaldb.simulation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.global.ForwardMessage;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.simulation.command.BootCmd;
import se.sics.caracaldb.simulation.command.Experiment;
import se.sics.caracaldb.simulation.command.GetCmd;
import se.sics.caracaldb.simulation.command.OpCmd;
import se.sics.caracaldb.simulation.command.PutCmd;
import se.sics.caracaldb.simulation.command.TerminateCmd;
import se.sics.caracaldb.simulation.command.ValidateCmd;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.HostManager;
import se.sics.caracaldb.system.HostManagerInit;
import se.sics.caracaldb.system.Launcher;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kompics;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.VirtualNetworkChannel;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.virtual.networkmodel.HostAddress;
import se.sics.kompics.virtual.simulator.MessageDestinationFilter;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class SimulatorComponent extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(SimulatorComponent.class);
    private static final Random RAND = new Random();
    Positive<Experiment> simulator = requires(Experiment.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    // instance
    private TreeSet<Integer> portsInUse = new TreeSet<Integer>();
    private InetAddress localIP;
    private Configuration baseConfig;
    private Address receiver;
    private Address target;

    private ValidationStore valStore;

    Positive<Experiment2Port> expExecutor = requires(Experiment2Port.class);

    public SimulatorComponent(SimulatorComponentInit init) {
        baseConfig = init.config;

        try {
            localIP = InetAddress.getLocalHost();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex.getMessage());
        }

        valStore = new ValidationStore();
        receiver = new Address(localIP, getFreePort(), null);
        TimestampIdFactory.init(receiver);

        // Subscriptions
        //system nodes simulation
        subscribe(nodeBootHandler, simulator);
        subscribe(killSystemHandler, simulator);

        if (SimulationHelper.type.equals(SimulationHelper.ExpType.NO_RESULT)) {
            subscribe(getHandler, simulator);
            subscribe(putHandler, simulator);
            subscribe(validateHandler, simulator);
            subscribe(terminateExpHandler, simulator);

            Component respReceiver = create(ResponseReceiver.class, new ResponseReceiverInit(valStore));
            connect(respReceiver.getNegative(Network.class), net, new MessageDestinationFilter(new HostAddress(receiver)));
        } else if (SimulationHelper.type.equals(SimulationHelper.ExpType.WITH_RESULT)) {
            subscribe(experimentOpHandler, simulator);
            subscribe(caracalOpHandler, expExecutor);
            subscribe(terminateExp2Handler, simulator);

            ValidationStore2 resultValidator = new ValidationStore2();
            SimulationHelper.resultValidator = resultValidator;
            Component experimentExecutor = create(Experiment2.class, new Experiment2Init(resultValidator));
            connect(experimentExecutor.getNegative(Network.class), net, new MessageDestinationFilter(new HostAddress(receiver)));
            connect(expExecutor, experimentExecutor.getNegative(Experiment2Port.class));
        }
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

    private Key randomKey(int size) {
        int s = size;
        if (size == -1) {
            s = Math.abs(RAND.nextInt(1000));
        }
        byte[] bytes = new byte[s];
        RAND.nextBytes(bytes);
        // don't write in the 00 XX... key range
        // it's reserved
        if (bytes[0] == 0) {
            bytes[0] = 1;
        }
        return new Key(bytes);
    }

    //*****************
    //system simulation
    //*****************
    Handler<BootCmd> nodeBootHandler = new Handler<BootCmd>() {
        @Override
        public void handle(BootCmd event) {
            LOG.info("Booting up {} nodes.", event.nodeCount);
            if (event.nodeCount < 3) {
                throw new RuntimeException("FATAL: Need to start at least 3 hosts!");
            }
            int n = event.nodeCount - 1;

            int bootstrapPort = getFreePort();
            baseConfig = Configuration.Factory.modify(baseConfig)
                    .setBootstrapServer(new Address(localIP, bootstrapPort, null))
                    .finalise();
            bootBootstrapNode();

            for (int i = 0; i < n; i++) {
                int port = getFreePort();
                bootNode(port);
            }

        }
    };

    private void bootBootstrapNode() {
        Configuration myConf = Configuration.Factory.modify(baseConfig)
                .setIp(baseConfig.getBootstrapServer().getIp())
                .setPort(baseConfig.getBootstrapServer().getPort())
                .finalise();

        Address netSelf = new Address(myConf.getIp(), myConf.getPort(), null);

        VirtualNetworkChannel vnc = VirtualNetworkChannel.connect(net,
                new MessageDestinationFilter(new HostAddress(netSelf)));
        Component manager = create(HostManager.class, new HostManagerInit(myConf, netSelf, vnc));

        connect(manager.getNegative(Timer.class), timer);

        trigger(Start.event, manager.control());
        target = netSelf;
    }

    private void bootNode(int port) {
        Configuration myConf = Configuration.Factory.modify(baseConfig)
                .setIp(baseConfig.getBootstrapServer().getIp())
                .setPort(port)
                .finalise();

        Address netSelf = new Address(myConf.getIp(), myConf.getPort(), null);

        VirtualNetworkChannel vnc = VirtualNetworkChannel.connect(net,
                new MessageDestinationFilter(new HostAddress(netSelf)));
        Component manager = create(HostManager.class, new HostManagerInit(myConf, netSelf, vnc));

        connect(manager.getNegative(Timer.class), timer);

        trigger(Start.event, manager.control());
    }

    Handler<TerminateExperiment> killSystemHandler = new Handler<TerminateExperiment>() {
        @Override
        public void handle(TerminateExperiment event) {
            LOG.info("Terminating Experiment.");
            Kompics.forceShutdown();
        }
    };
    //*************************************
    //experiment - put get operation success
    //**************************************
    Handler<GetCmd> getHandler = new Handler<GetCmd>() {
        @Override
        public void handle(GetCmd event) {
            Key k = randomKey(8);
            GetRequest getr = new GetRequest(TimestampIdFactory.get().newId(), k);
            valStore.request(getr);
            CaracalMsg msg = new CaracalMsg(receiver, target, getr);
            ForwardMessage fMsg = new ForwardMessage(receiver, target, k, msg);
            trigger(fMsg, net);
            LOG.debug("Sent GET {}", getr);
        }
    };
    Handler<PutCmd> putHandler = new Handler<PutCmd>() {
        @Override
        public void handle(PutCmd event) {
            Key k = randomKey(8);
            Key val = randomKey(32);
            PutRequest putr = new PutRequest(TimestampIdFactory.get().newId(), k, val.getArray());
            valStore.request(putr);
            CaracalMsg msg = new CaracalMsg(receiver, target, putr);
            ForwardMessage fMsg = new ForwardMessage(receiver, target, k, msg);
            trigger(fMsg, net);
            LOG.debug("Sent PUT {}", putr);
        }
    };

    Handler<ValidateCmd> validateHandler = new Handler<ValidateCmd>() {
        @Override
        public void handle(ValidateCmd event) {
            SimulationHelper.setValidator(valStore);
            if (valStore.isDone()) {
                LOG.info("Placed Validation Store.");
                trigger(new TerminateExperiment(), simulator);
            }
        }
    };

    Handler<TerminateCmd> terminateExpHandler = new Handler<TerminateCmd>() {
        @Override
        public void handle(TerminateCmd event) {
            LOG.info("Got termination command.");
            trigger(new TerminateExperiment(), simulator);
        }
    };

    //*******************************************
    //experiment put/rangequery with result check
    //*******************************************
    Handler<OpCmd> experimentOpHandler = new Handler<OpCmd>() {

        @Override
        public void handle(OpCmd op) {
            trigger(op, expExecutor);
        }
    };

    Handler<CaracalOp> caracalOpHandler = new Handler<CaracalOp>() {

        @Override
        public void handle(CaracalOp op) {
            //should get rid of this
            Key key;
            if (op instanceof PutRequest) {
                key = ((PutRequest) op).key;
            } else if (op instanceof RangeQuery.Request) {
                key = ((RangeQuery.Request) op).initRange.begin;
            } else {
                LOG.error("unexpected op - logic error");
                System.exit(1);
                return;
            }

            CaracalMsg msg = new CaracalMsg(receiver, target, op);
            ForwardMessage fMsg = new ForwardMessage(receiver, target, key, msg);
            trigger(fMsg, net);
        }
    };

    Handler<TerminateCmd> terminateExp2Handler = new Handler<TerminateCmd>() {

        @Override
        public void handle(TerminateCmd event) {
            SimulationHelper.resultValidator.endExperiment();
            LOG.info("Got termination command.");
            trigger(event, expExecutor);
        }
    };
}
