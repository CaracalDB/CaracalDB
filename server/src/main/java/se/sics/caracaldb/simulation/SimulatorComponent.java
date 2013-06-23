/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.PutRequest;
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
    private ValidationStore valStore;
    private Address receiver;
    private Address target;

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

        Component respReceiver = create(ResponseReceiver.class, new ResponseReceiverInit(valStore));
        connect(respReceiver.getNegative(Network.class), net, new MessageDestinationFilter(new HostAddress(receiver)));
        // Subscriptions

        subscribe(bootHandler, simulator);
        subscribe(getHandler, simulator);
        subscribe(putHandler, simulator);
        subscribe(terminationHandler, simulator);
        subscribe(terminateHandler, simulator);
        subscribe(validateHandler, simulator);
    }
    Handler<BootCmd> bootHandler = new Handler<BootCmd>() {
        @Override
        public void handle(BootCmd event) {
            LOG.info("Booting up {} nodes.", event.nodeCount);
            if (event.nodeCount < 3) {
                throw new RuntimeException("FATAL: Need to start at least 3 hosts!");
            }
            int n = event.nodeCount - 1;

            int bootstrapPort = getFreePort();
            baseConfig.setBootstrapServer(new Address(localIP, bootstrapPort, null));
            bootBootstrapNode();

            for (int i = 0; i < n; i++) {
                int port = getFreePort();
                bootNode(port);
            }

        }
    };
    Handler<ValidateCmd> validateHandler = new Handler<ValidateCmd>() {
        @Override
        public void handle(ValidateCmd event) {
            Launcher.setValidator(valStore);
            if (valStore.isDone()) {
                LOG.info("Placed Validation Store.");
                trigger(new TerminateExperiment(), simulator);
            }
        }
    };
//    Handler<OpCmd> opHandler = new Handler<OpCmd>() {
//
//        @Override
//        public void handle(OpCmd event) {
//            
//        }
//    };
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
    Handler<TerminateExperiment> terminateHandler = new Handler<TerminateExperiment>() {
        @Override
        public void handle(TerminateExperiment event) {
            LOG.info("Terminating Experiment.");
            Kompics.forceShutdown();
        }
    };
    Handler<TerminateCmd> terminationHandler = new Handler<TerminateCmd>() {
        @Override
        public void handle(TerminateCmd event) {
            LOG.info("Got termination command.");
            trigger(new TerminateExperiment(), simulator);
        }
    };

    private void bootBootstrapNode() {
        Configuration myConf = (Configuration) baseConfig.clone();
        myConf.setIp(baseConfig.getBootstrapServer().getIp());
        myConf.setPort(baseConfig.getBootstrapServer().getPort());

        Address netSelf = new Address(myConf.getIp(), myConf.getPort(), null);

        VirtualNetworkChannel vnc = VirtualNetworkChannel.connect(net,
                new MessageDestinationFilter(new HostAddress(netSelf)));
        Component manager = create(HostManager.class, new HostManagerInit(myConf, netSelf, vnc));

        connect(manager.getNegative(Timer.class), timer);

        trigger(Start.event, manager.control());
        target = netSelf;
    }

    private void bootNode(int port) {
        Configuration myConf = (Configuration) baseConfig.clone();
        myConf.setIp(baseConfig.getBootstrapServer().getIp());
        myConf.setPort(port);

        Address netSelf = new Address(myConf.getIp(), myConf.getPort(), null);

        VirtualNetworkChannel vnc = VirtualNetworkChannel.connect(net,
                new MessageDestinationFilter(new HostAddress(netSelf)));
        Component manager = create(HostManager.class, new HostManagerInit(myConf, netSelf, vnc));

        connect(manager.getNegative(Timer.class), timer);

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
}
