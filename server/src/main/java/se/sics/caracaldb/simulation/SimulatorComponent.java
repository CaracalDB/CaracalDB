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
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.global.ForwardMessage;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.simulation.command.BootCmd;
import se.sics.caracaldb.simulation.command.Experiment;
import se.sics.caracaldb.simulation.command.OpCmd;
import se.sics.caracaldb.simulation.command.TerminateCmd;
import se.sics.caracaldb.simulation.command.ValidateCmd;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.HostManager;
import se.sics.caracaldb.system.HostManagerInit;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kompics;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.virtual.VirtualNetworkChannel;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.virtual.networkmodel.HostAddress;
import se.sics.kompics.virtual.simulator.MessageDestinationFilter;

/**
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

    Positive<ExperimentPort> expExecutor = requires(ExperimentPort.class);

    public SimulatorComponent(SimulatorComponentInit init) {
        baseConfig = init.config;

        try {
            localIP = InetAddress.getLocalHost();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex.getMessage());
        }

        receiver = new Address(localIP, getFreePort(), null);
        TimestampIdFactory.init(receiver);

        // Subscriptions
        //system nodes simulation
        subscribe(nodeBootHandler, simulator);
        subscribe(killSystemHandler, simulator);
        //experiments
        subscribe(experimentOpHandler, simulator);
        subscribe(validateHandler, simulator);
        subscribe(terminateHandler, simulator);
        subscribe(experimentEndedHandler, expExecutor);
        subscribe(caracalOpHandler, expExecutor);

        Component experimentExecutor;
        if (SimulationHelper.type.equals(SimulationHelper.ExpType.NO_RESULT)) {
            experimentExecutor = create(Experiment1.class, new Experiment1Init());
        } else if (SimulationHelper.type.equals(SimulationHelper.ExpType.WITH_RESULT)) {
            experimentExecutor = create(Experiment2.class, new Experiment2Init());
        } else {
            LOG.error("unknown experiment");
            System.exit(1);
            return;
        }
        connect(experimentExecutor.getNegative(Network.class), net, new MessageDestinationFilter(new HostAddress(receiver)));
        connect(expExecutor.getPair(), experimentExecutor.getPositive(ExperimentPort.class));
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
        Configuration myConf = Configuration.Factory.modifyWithOtherDB(baseConfig, "bootStrap/")
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
        Configuration myConf = Configuration.Factory.modifyWithOtherDB(baseConfig, String.valueOf(port)+"/")
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
            LOG.info("kill system, terminate experiment.");
            Kompics.forceShutdown();
        }
    };
    
     Handler<TerminateCmd> terminateHandler = new Handler<TerminateCmd>() {
        @Override
        public void handle(TerminateCmd event) {
            LOG.info("Got termination command.");
            trigger(new TerminateExperiment(), simulator);
        }
    };

    //***************************
    //experiment forward handlers
    //***************************
    Handler<TerminateExperiment> experimentEndedHandler = new Handler<TerminateExperiment>() {

        @Override
        public void handle(TerminateExperiment event) {
            LOG.info("experiment ended");
            trigger(event, simulator);
        }
    };

    Handler<OpCmd> experimentOpHandler = new Handler<OpCmd>() {

        @Override
        public void handle(OpCmd op) {
            System.out.println("Timestamp: " + System.currentTimeMillis());
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
            } else if (op instanceof GetRequest) {
                key = ((GetRequest) op).key;
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

    Handler<ValidateCmd> validateHandler = new Handler<ValidateCmd>() {

        @Override
        public void handle(ValidateCmd event) {
            LOG.info("Got validation command.");
            trigger(event, expExecutor);
        }
    };
}
