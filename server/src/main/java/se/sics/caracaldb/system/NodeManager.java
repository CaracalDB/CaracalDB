/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class NodeManager extends ComponentDefinition {

    private static final Logger log = LoggerFactory.getLogger(NodeManager.class);
    private Configuration config;
    private VirtualSharedComponents vsc;
    private Address self;
    private Positive<Network> networkPort;
    private ComponentProxy proxy = new ComponentProxy() {
        @Override
        public <P extends PortType> void trigger(Event e, Port<P> p) {
            NodeManager.this.trigger(e, p);
        }

        @Override
        public void destroy(Component component) {
            NodeManager.this.destroy(component);
        }

        @Override
        public <P extends PortType> Channel<P> connect(Positive<P> positive, Negative<P> negative) {
            return NodeManager.this.connect(positive, negative);
        }

        @Override
        public <P extends PortType> Channel<P> connect(Negative<P> negative, Positive<P> positive) {
            return NodeManager.this.connect(negative, positive);
        }

        @Override
        public <P extends PortType> void disconnect(Negative<P> negative, Positive<P> positive) {
            NodeManager.this.disconnect(negative, positive);
        }

        @Override
        public <P extends PortType> void disconnect(Positive<P> positive, Negative<P> negative) {
            NodeManager.this.disconnect(positive, negative);
        }

        @Override
        public Negative<ControlPort> getControlPort() {
            return NodeManager.this.control;
        }

        @Override
        public <T extends ComponentDefinition> Component create(Class<T> definition, Init<T> initEvent) {
            return NodeManager.this.create(definition, initEvent);
        }

        @Override
        public <T extends ComponentDefinition> Component create(Class<T> definition, None initEvent) {
            return NodeManager.this.create(definition, initEvent);
        }
    };

    public NodeManager(NodeManagerInit init) {
        networkPort = requires(Network.class);


        subscribe(stopNodeHandler, networkPort);


        // INIT
        vsc = init.vsc;
        config = init.config;
        self = vsc.getSelf();

        log.debug("Setting up VNode: " + self);

        for (VirtualComponentHook hook : config.getVirtualHooks()) {
            hook.setUp(vsc, proxy);
        }
    }
    private final Handler<StopVNode> stopNodeHandler = new Handler<StopVNode>() {
        @Override
        public void handle(StopVNode event) {
            log.info("Node shutting down (" + self + ")");
            trigger(Stop.event, control.getPair());
        }
    };

    @Override
    public void tearDown() {
        for (VirtualComponentHook hook : config.getVirtualHooks()) {
            hook.tearDown(vsc, proxy);
        }
        vsc.disconnectNetwork(this.getComponentCore());
    }
}
