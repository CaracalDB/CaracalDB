/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.simulation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.simulation.command.BootCmd;
import se.sics.caracaldb.simulation.command.Experiment;
import se.sics.caracaldb.simulation.command.TerminateCmd;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.HostManager;
import se.sics.caracaldb.system.HostManagerInit;
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
    
    Positive<Experiment> simulator = requires(Experiment.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    
    private TreeSet<Integer> portsInUse = new TreeSet<Integer>();
    private InetAddress localIP;
    private Configuration baseConfig;
    
    public SimulatorComponent(SimulatorComponentInit init) {
        baseConfig = init.config;
        
        try {
            localIP = InetAddress.getLocalHost();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex.getMessage());
        }
        
        // Subscriptions
        
        subscribe(bootHandler, simulator);
        subscribe(terminationHandler, simulator);
        subscribe(terminateHandler, simulator);
    }
    
    Handler<BootCmd> bootHandler = new Handler<BootCmd>(){

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
    
}
