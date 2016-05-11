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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
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
import se.sics.caracaldb.paxos.Paxos;
import se.sics.caracaldb.paxos.PaxosInit;
import se.sics.caracaldb.persistence.Database;
import se.sics.caracaldb.replication.linearisable.ExecutionEngine;
import se.sics.caracaldb.replication.linearisable.ExecutionEngineInit;
import se.sics.caracaldb.replication.linearisable.Replication;
import se.sics.caracaldb.replication.log.ReplicatedLog;
import se.sics.caracaldb.store.PersistentStore;
import se.sics.caracaldb.store.PersistentStoreInit;
import se.sics.caracaldb.store.Store;
import se.sics.caracaldb.system.Configuration.NodePhase;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
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

    public NodeManager(NodeManagerInit init) throws ClassNotFoundException, InstantiationException {
        networkPort = requires(Network.class);
        maintenancePort = requires(MaintenanceService.class);

        subscribe(stopNodeHandler, networkPort);

        // INIT
        vsc = init.vsc;
        config = init.config;
        self = vsc.getSelf();

        LOG.debug("Setting up VNode: " + self);

        // Create stores on vnode level so they get started and stopped with the vnode itself
        if (vsc.getDbLevel(config) == Database.Level.VNODE) {
            Component store = create(PersistentStore.class, new PersistentStoreInit(config.getDBMan().getInstance(vsc.getDbType(config))));
            vsc.setStore(store.getPositive(Store.class));
        }

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
            suicide();
        }
    };
    Handler<MaintenanceMsg> maintenanceHandler = new Handler<MaintenanceMsg>() {
        @Override
        public void handle(MaintenanceMsg event) {
            if (event.op instanceof NodeJoin) {
                LOG.debug("{}: Joining: {}", self, event.op);
                NodeJoin join = (NodeJoin) event.op;
                Component methCat = create(MethCat.class,
                        new Meth(self, join.responsibility, join.view,
                                config.getMilliseconds("caracal.stats.nodeInterval")));
                View repView = join.dataTransfer ? null : join.view;
                //LOG.debug("NODEJOIN {} - {}", join.dataTransfer, repView);
                Component replication = create(ExecutionEngine.class,
                        new ExecutionEngineInit(repView, self,
                                join.responsibility,
                                config.getMilliseconds("caracal.network.keepAlivePeriod")));
                Component paxos = create(Paxos.class,
                        new PaxosInit(repView, join.quorum,
                                config.getMilliseconds("caracal.network.keepAlivePeriod"), self));
                // methcat
                vsc.connectNetwork(methCat);
                connect(methCat.getNegative(Replication.class), replication.getPositive(Replication.class));
                connect(methCat.getNegative(LookupService.class), vsc.getLookup());
                connect(methCat.getNegative(MaintenanceService.class), maintenancePort);
                connect(methCat.getNegative(Timer.class), vsc.getTimer());
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
