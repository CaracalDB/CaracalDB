/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.bootstrap.BootUp;
import se.sics.caracaldb.bootstrap.Bootstrap;
import se.sics.caracaldb.bootstrap.BootstrapCInit;
import se.sics.caracaldb.bootstrap.BootstrapClient;
import se.sics.caracaldb.bootstrap.BootstrapSInit;
import se.sics.caracaldb.bootstrap.BootstrapServer;
import se.sics.caracaldb.bootstrap.Bootstrapped;
import se.sics.caracaldb.global.CatHerder;
import se.sics.caracaldb.global.CatHerderInit;
import se.sics.caracaldb.global.LookupService;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ControlPort;
import se.sics.kompics.Event;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Init.None;
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
import se.sics.kompics.network.grizzly.ConstantQuotaAllocator;
import se.sics.kompics.network.grizzly.GrizzlyNetwork;
import se.sics.kompics.network.grizzly.GrizzlyNetworkInit;
import se.sics.kompics.network.grizzly.kryo.KryoMessage;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class HostManager extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(HostManager.class);
    private HostSharedComponents sharedComponents;
    public final Configuration config;
    private Positive<Network> net;
    private Positive<Timer> timer;
    private Positive<Bootstrap> bootPort;
    private ComponentProxy proxy = new ComponentProxy() {
        @Override
        public <P extends PortType> void trigger(Event e, Port<P> p) {
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

    public HostManager(HostManagerInit init) throws UnknownHostException {
        config = init.config;
        netSelf = init.netSelf;

        net = requires(Network.class);
        timer = requires(Timer.class);
        bootPort = requires(Bootstrap.class);


        sharedComponents = new HostSharedComponents();
        sharedComponents.setSelf(netSelf);


        VirtualNetworkChannel vnc = init.vnc;
        sharedComponents.setNetwork(vnc);
        sharedComponents.setTimer(timer);
        sharedComponents.connectNetwork(this.getComponentCore());

        for (ComponentHook hook : config.getComponentHooks()) {
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
            log.info("Starting new VNode: " + nodeAddr);

            VirtualSharedComponents vsc = new VirtualSharedComponents(event.nodeId);

            vsc.setSelf(nodeAddr);
            vsc.setNetwork(sharedComponents.getNet());
            if (bootstrapped) {
                vsc.setLookup(catHerder.getPositive(LookupService.class));
            }

            Component nodeMan = create(NodeManager.class, new NodeManagerInit(vsc, config));

            vsc.connectNetwork(nodeMan);

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

                log.debug("{} is bootstrapped", netSelf);

                startCatHerder(event);
            }
        };
        subscribe(bootHandler, bootPort);
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

                log.debug("{} is bootstrapped", netSelf);

                startCatHerder(event);
            }
        };
        subscribe(bootHandler, bootPort);
    }

    private void startCatHerder(Bootstrapped event) {
        catHerder = create(CatHerder.class, new CatHerderInit(event, netSelf));
        sharedComponents.connectNetwork(catHerder);

        trigger(Start.event, catHerder.control());

        log.debug("{} starting CatHerder", netSelf);
        
//        StringBuilder sb = new StringBuilder();
//        event.lut.printFormat(sb);
//        System.out.println(sb.toString());

        // might not be the most obvious place...but at least I only have to write it once
        bootstrapped = true;
    }

    @Override
    public void tearDown() {
        for (ComponentHook hook : config.getComponentHooks()) {
            hook.tearDown(sharedComponents, proxy);
        }
    }
}
