/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.global.LookupService;
import se.sics.caracaldb.global.MaintenanceMsg;
import se.sics.caracaldb.global.MaintenanceService;
import se.sics.caracaldb.global.NodeBooted;
import se.sics.caracaldb.global.NodeJoin;
import se.sics.caracaldb.global.NodeSynced;
import se.sics.caracaldb.operations.Meth;
import se.sics.caracaldb.operations.MethCat;
import se.sics.caracaldb.replication.log.ReplicatedLog;
import se.sics.caracaldb.paxos.Paxos;
import se.sics.caracaldb.paxos.PaxosInit;
import se.sics.caracaldb.replication.linearisable.ExecutionEngine;
import se.sics.caracaldb.replication.linearisable.ExecutionEngineInit;
import se.sics.caracaldb.replication.linearisable.Replication;
import se.sics.caracaldb.store.Store;
import se.sics.caracaldb.system.Configuration.NodePhase;
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
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class NodeManager extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(NodeManager.class);
    private Configuration config;
    private VirtualSharedComponents vsc;
    private Address self;
    private Positive<Network> networkPort;
    private Positive<MaintenanceService> maintenancePort;
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
        maintenancePort = requires(MaintenanceService.class);


        subscribe(stopNodeHandler, networkPort);


        // INIT
        vsc = init.vsc;
        config = init.config;
        self = vsc.getSelf();

        LOG.debug("Setting up VNode: " + self);


        for (VirtualComponentHook hook : config.getVirtualHooks(NodePhase.INIT)) {
            hook.setUp(vsc, proxy);
        }

        // subscriptions
        subscribe(stopNodeHandler, networkPort);
        subscribe(startHandler, control);
        subscribe(maintenanceHandler, networkPort);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            trigger(new NodeBooted(self), maintenancePort);
        }
    };
    Handler<StopVNode> stopNodeHandler = new Handler<StopVNode>() {
        @Override
        public void handle(StopVNode event) {
            LOG.info("Node shutting down (" + self + ")");
            trigger(Stop.event, control.getPair());
        }
    };
    Handler<MaintenanceMsg> maintenanceHandler = new Handler<MaintenanceMsg>() {
        @Override
        public void handle(MaintenanceMsg event) {
            if (event.op instanceof NodeJoin) {
                LOG.debug("{}: Joining: {}", self, event.op);
                NodeJoin join = (NodeJoin) event.op;
                Component methCat = create(MethCat.class,
                        new Meth(self, join.responsibility, join.view));
                View repView = join.dataTransfer ? null : join.view;
                //LOG.debug("NODEJOIN {} - {}", join.dataTransfer, repView);
                Component replication = create(ExecutionEngine.class,
                        new ExecutionEngineInit(repView, self,
                        join.responsibility,
                        config.getMilliseconds("caracal.network.keepAlivePeriod"),
                        config.getInt("caracal.network.dataMessageSize")));
                Component paxos = create(Paxos.class,
                        new PaxosInit(repView, join.quorum, 
                        config.getMilliseconds("caracal.network.keepAlivePeriod"), self));
                // methcat
                vsc.connectNetwork(methCat);
                connect(methCat.getNegative(Replication.class), replication.getPositive(Replication.class));
                connect(methCat.getNegative(LookupService.class), vsc.getLookup());
                // repl
                vsc.connectNetwork(replication);
                connect(replication.getNegative(Store.class), vsc.getStore());
                connect(replication.getNegative(Timer.class), vsc.getTimer());
                connect(replication.getNegative(ReplicatedLog.class), paxos.getPositive(ReplicatedLog.class));
                // paxos
                vsc.connectNetwork(paxos);
                connect(paxos.getNegative(EventualFailureDetector.class), vsc.getFailureDetector());

                // Start!
                trigger(Start.event, paxos.control());
                trigger(Start.event, replication.control());
                trigger(Start.event, methCat.control());

                for (VirtualComponentHook hook : config.getVirtualHooks(NodePhase.JOIN)) {
                    hook.setUp(vsc, proxy);
                }
            } else if (event.op instanceof NodeSynced) {
                for (VirtualComponentHook hook : config.getVirtualHooks(NodePhase.SYNCED)) {
                    hook.setUp(vsc, proxy);
                }
            }
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
