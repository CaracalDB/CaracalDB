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
package se.sics.caracaldb.system;

import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.bootstrap.Bootstrap;
import se.sics.caracaldb.bootstrap.BootstrapCInit;
import se.sics.caracaldb.bootstrap.BootstrapClient;
import se.sics.caracaldb.bootstrap.BootstrapSInit;
import se.sics.caracaldb.bootstrap.BootstrapServer;
import se.sics.caracaldb.bootstrap.Bootstrapped;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.global.CatHerder;
import se.sics.caracaldb.global.CatHerderInit;
import se.sics.caracaldb.global.LookupService;
import se.sics.caracaldb.global.MaintenanceService;
import se.sics.caracaldb.store.PersistentStore;
import se.sics.caracaldb.store.PersistentStoreInit;
import se.sics.caracaldb.store.Store;
import se.sics.caracaldb.system.Configuration.SystemPhase;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ControlPort;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Init.None;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Port;
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.Stopped;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.VirtualNetworkChannel;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class HostManager extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(HostManager.class);
    private HostSharedComponents sharedComponents;
    public final Configuration config;
    private Positive<Network> net;
    private Positive<Timer> timer;
    private Positive<EventualFailureDetector> fd;
    private Positive<Bootstrap> bootPort;
    private ComponentProxy proxy = new ComponentProxy() {
        @Override
        public <P extends PortType> void trigger(KompicsEvent e, Port<P> p) {
            HostManager.this.trigger(e, p);
        }

        @Override
        public void destroy(Component component) {
            HostManager.this.destroy(component);
        }

        @Override
        public <P extends PortType> Channel<P> connect(Positive<P> positive, Negative<P> negative) {
            return HostManager.this.connect(positive, negative);
        }

        @Override
        public <P extends PortType> Channel<P> connect(Negative<P> negative, Positive<P> positive) {
            return HostManager.this.connect(negative, positive);
        }

        @Override
        public <P extends PortType> void disconnect(Negative<P> negative, Positive<P> positive) {
            HostManager.this.disconnect(negative, positive);
        }

        @Override
        public <P extends PortType> void disconnect(Positive<P> positive, Negative<P> negative) {
            HostManager.this.disconnect(positive, negative);
        }

        @Override
        public Negative<ControlPort> getControlPort() {
            return HostManager.this.control;
        }

        @Override
        public <T extends ComponentDefinition> Component create(Class<T> definition, Init<T> initEvent) {
            return HostManager.this.create(definition, initEvent);
        }

        @Override
        public <T extends ComponentDefinition> Component create(Class<T> definition, None initEvent) {
            return HostManager.this.create(definition, initEvent);
        }
    };
    private Address netSelf;
    private boolean bootstrapped = false;
    private Component catHerder;
    private Component store;

    public HostManager(HostManagerInit init) throws UnknownHostException {
        config = init.config;
        netSelf = init.netSelf;

        net = requires(Network.class);
        timer = requires(Timer.class);
        fd = requires(EventualFailureDetector.class);
        bootPort = requires(Bootstrap.class);

        store = create(PersistentStore.class, new PersistentStoreInit(config.getDB()));
        LOG.info("{}: Created store: {}", netSelf, config.getDB());


        sharedComponents = new HostSharedComponents();
        sharedComponents.setSelf(netSelf);


        VirtualNetworkChannel vnc = init.vnc;
        sharedComponents.setNetwork(vnc);
        sharedComponents.setTimer(timer);
        sharedComponents.setFailureDetector(fd);
        sharedComponents.connectNetwork(this.getComponentCore());
        sharedComponents.setStore(store.getPositive(Store.class));

        // INIT phase
        for (ComponentHook hook : config.getHostHooks(SystemPhase.INIT)) {
            hook.setUp(sharedComponents, proxy);
        }


        subscribe(stoppedHandler, control);
        subscribe(startNodeHandler, net);
        

        if (config.isBoot()) {
            if (netSelf.equals(config.getBootstrapServer())) {
                startBootstrapServer();
            } else {
                startBootstrapClient();
            }
        }
    }
    private Handler<StartVNode> startNodeHandler = new Handler<StartVNode>() {
        @Override
        public void handle(StartVNode event) {
            Address nodeAddr = netSelf.newVirtual(event.nodeId);
            LOG.info("Starting new VNode: " + nodeAddr);

            VirtualSharedComponents vsc = new VirtualSharedComponents(event.nodeId);

            vsc.setSelf(nodeAddr);
            vsc.setNetwork(sharedComponents.getNet());
            vsc.setFailureDetector(fd);
            vsc.setTimer(timer);
            if (bootstrapped) {
                vsc.setLookup(catHerder.getPositive(LookupService.class));
                vsc.setStore(store.getPositive(Store.class));
                vsc.setMaintenance(catHerder.getPositive(MaintenanceService.class));
            }

            Component nodeMan = create(NodeManager.class, new NodeManagerInit(vsc, config));

            vsc.connectNetwork(nodeMan);
            if (bootstrapped) {
                connect(nodeMan.getNegative(MaintenanceService.class), vsc.getMaintenance());
            }

            trigger(Start.event, nodeMan.control());
        }
    };
    private Handler<Stopped> stoppedHandler = new Handler<Stopped>() {
        @Override
        public void handle(Stopped event) {
            destroy(event.component);
        }
    };

    private void startBootstrapClient() {
        final Component bootstrapClient = create(BootstrapClient.class, new BootstrapCInit(netSelf, config));
        connect(bootstrapClient.getNegative(Timer.class), sharedComponents.getTimer());
        sharedComponents.connectNetwork(bootstrapClient);
        connect(bootPort.getPair(), bootstrapClient.getPositive(Bootstrap.class));

        Handler<Bootstrapped> bootHandler = new Handler<Bootstrapped>() {
            @Override
            public void handle(Bootstrapped event) {

                trigger(Stop.event, bootstrapClient.control());
                sharedComponents.disconnectNetwork(bootstrapClient);
                disconnect(bootPort.getPair(), bootstrapClient.getPositive(Bootstrap.class));
                disconnect(bootstrapClient.getNegative(Timer.class), sharedComponents.getTimer());
                unsubscribe(this, bootPort);

                LOG.debug("{} is bootstrapped", netSelf);

                startCatHerder(event);
            }
        };
        subscribe(bootHandler, bootPort);
        
        // CLIENT only
        for (ComponentHook hook : config.getHostHooks(SystemPhase.BOOTSTRAP_CLIENT)) {
            hook.setUp(sharedComponents, proxy);
        }
    }

    private void startBootstrapServer() {
        final Component bootstrapServer = create(BootstrapServer.class, new BootstrapSInit(netSelf, config));
        connect(bootstrapServer.getNegative(Timer.class), sharedComponents.getTimer());
        sharedComponents.connectNetwork(bootstrapServer);
        connect(bootPort.getPair(), bootstrapServer.getPositive(Bootstrap.class));

        Handler<Bootstrapped> bootHandler = new Handler<Bootstrapped>() {
            @Override
            public void handle(Bootstrapped event) {



                //TODO send stuff to ConfigManager
                trigger(Stop.event, bootstrapServer.control());
                sharedComponents.disconnectNetwork(bootstrapServer);
                disconnect(bootPort.getPair(), bootstrapServer.getPositive(Bootstrap.class));
                disconnect(bootstrapServer.getNegative(Timer.class), sharedComponents.getTimer());
                unsubscribe(this, bootPort);

                LOG.debug("{} is bootstrapped", netSelf);

                startCatHerder(event);
            }
        };
        subscribe(bootHandler, bootPort);
        
        // SERVER only
        for (ComponentHook hook : config.getHostHooks(SystemPhase.BOOTSTRAP_SERVER)) {
            hook.setUp(sharedComponents, proxy);
        }
    }

    private void startCatHerder(Bootstrapped event) {
        catHerder = create(CatHerder.class, new CatHerderInit(event, netSelf, config));
        sharedComponents.connectNetwork(catHerder);
        connect(catHerder.getNegative(EventualFailureDetector.class), fd);
        connect(catHerder.getNegative(Timer.class), timer);
        connect(catHerder.getNegative(Store.class), sharedComponents.getStore());
        
        trigger(Start.event, catHerder.control());

        LOG.debug("{} starting CatHerder", netSelf);

//        StringBuilder sb = new StringBuilder();
//        event.lut.printFormat(sb);
//        System.out.println(sb.toString());

        // might not be the most obvious place...but at least I only have to write it once
        bootstrapped = true;
        
        // BOOTSTRAPPED phase
        for (ComponentHook hook : config.getHostHooks(SystemPhase.BOOTSTRAPPED)) {
            hook.setUp(sharedComponents, proxy);
        }
    }

    @Override
    public void tearDown() {
        for (ComponentHook hook : config.getHostHooks()) {
            hook.tearDown(sharedComponents, proxy);
        }
    }
}
