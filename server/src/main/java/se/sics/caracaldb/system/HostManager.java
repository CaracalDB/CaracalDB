/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ControlPort;
import se.sics.kompics.Event;
import se.sics.kompics.Handler;
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

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class HostManager extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(HostManager.class);
    private HostSharedComponents sharedComponents;
    public final Configuration config;
    private Positive<Network> networkPort;
    private ComponentProxy proxy = new ComponentProxy() {
        @Override
        public <P extends PortType> void trigger(Event e, Port<P> p) {
            HostManager.this.trigger(e, p);
        }

        @Override
        public Component create(Class<? extends ComponentDefinition> definition) {
            return HostManager.this.create(definition);
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
    };
    private Address netSelf;

    static {
        KryoMessage.register(StartVNode.class);
    }

    public HostManager() throws UnknownHostException {
        config = Launcher.getCurrentConfig();

        networkPort = requires(Network.class);


        sharedComponents = new HostSharedComponents();
        Component network = create(GrizzlyNetwork.class);
        netSelf = new Address(config.getIp(), config.getPort(), null);
        sharedComponents.setSelf(netSelf);
        trigger(new GrizzlyNetworkInit(netSelf, 8, 0, 0, 2 * 1024, 16 * 1024,
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                new ConstantQuotaAllocator(5)), network.control());
        VirtualNetworkChannel vnc = VirtualNetworkChannel.connect(network.getPositive(Network.class));
        sharedComponents.setNetwork(vnc);
        sharedComponents.connectNetwork(this.getComponentCore());

        for (ComponentHook hook : config.getComponentHooks()) {
            hook.setUp(sharedComponents, proxy);
        }


        subscribe(stoppedHandler, control);
        subscribe(startNodeHandler, networkPort);
    }
    private Handler<StartVNode> startNodeHandler = new Handler<StartVNode>() {
        @Override
        public void handle(StartVNode event) {
            Address nodeAddr = netSelf.newVirtual(event.nodeId);
            log.info("Starting new VNode: " + nodeAddr);

            VirtualSharedComponents vsc = new VirtualSharedComponents(event.nodeId);

            vsc.setSelf(nodeAddr);
            vsc.setNetwork(sharedComponents.getNet());

            Component nodeMan = create(NodeManager.class);
            
            vsc.connectNetwork(nodeMan);
            
            trigger(new NodeManagerInit(vsc, config), nodeMan.control());
            trigger(Start.event, nodeMan.control());
        }
    };
    
    private Handler<Stopped> stoppedHandler = new Handler<Stopped>() {
        @Override
        public void handle(Stopped event) {
            destroy(event.component);
        }
    };

    @Override
    public void tearDown() {
        for (ComponentHook hook : config.getComponentHooks()) {
            hook.tearDown(sharedComponents, proxy);
        }
    }
}
