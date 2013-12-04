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
package se.sics.caracaldb.simulation.system;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.simulation.common.msg.ConnectNode;
import se.sics.caracaldb.simulation.system.cmd.BootCmd;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.HostManager;
import se.sics.caracaldb.system.HostManagerInit;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
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
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class SystemComponent extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(SystemComponent.class);

    Negative<SystemPort> system = provides(SystemPort.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);

    private final TreeSet<Integer> portsInUse = new TreeSet<Integer>();
    private Configuration baseConfig;
    private InetAddress localIP;

    private Address connectNode = null;

    private boolean terminated = false;

    public SystemComponent(SystemComponentInit init) {
        baseConfig = init.config;
        try {
            localIP = InetAddress.getLocalHost();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex.getMessage());
        }

        subscribe(terminateHandler, system);
        subscribe(bootHandler, system);
        subscribe(connectNodeHandler, system);
    }

    Handler<TerminateExperiment> terminateHandler = new Handler<TerminateExperiment>() {
        @Override
        public void handle(TerminateExperiment event) {
            if (terminated) {
                LOG.debug("Terminated - dropping msg...");
                return;
            }
            terminated = true;
            LOG.info("terminated");
        }
    };

    Handler<BootCmd> bootHandler = new Handler<BootCmd>() {
        @Override
        public void handle(BootCmd event) {
            if (terminated) {
                LOG.debug("Terminated - dropping msg...");
                return;
            }
            
            LOG.info("Received BOOT CMD");
            LOG.info("Booting up {} nodes.", event.nodeCount);

            if (event.nodeCount < 3) {
                LOG.error("FATAL: Need to start at least 3 hosts!");
                throw new RuntimeException("FATAL: Need to start at least 3 hosts!");
            }
            int n = event.nodeCount - 1;

            bootBootstrapNode();

            for (int i = 0; i < n; i++) {
                int port = getFreePort();
                bootNode(port);
            }

        }
    };

    Handler<ConnectNode.Req> connectNodeHandler = new Handler<ConnectNode.Req>() {

        @Override
        public void handle(ConnectNode.Req event) {
            if (terminated) {
                LOG.debug("Terminated - dropping msg...");
                return;
            }
            
            trigger(new ConnectNode.Ind(connectNode), system);
        }

    };

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

    private void bootBootstrapNode() {
        int bootstrapPort = getFreePort();
        baseConfig = Configuration.Factory.modify(baseConfig)
                .setBootstrapServer(new Address(localIP, bootstrapPort, null))
                .finalise();
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
        setConnectNode(netSelf);
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

    private void setConnectNode(Address connectNode) {
        this.connectNode = connectNode;
        trigger(new ConnectNode.Ind(connectNode), system);
    }
}
