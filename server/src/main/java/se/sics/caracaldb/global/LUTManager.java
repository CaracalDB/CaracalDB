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
package se.sics.caracaldb.global;

import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.bootstrap.BootstrapRequest;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.replication.linearisable.ViewChange;
import se.sics.caracaldb.store.Store;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.StartVNode;
import se.sics.caracaldb.system.Stats;
import se.sics.caracaldb.system.Stats.Report;
import se.sics.caracaldb.system.StopVNode;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class LUTManager extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(LUTManager.class);
    private static final Random RAND = new Random();
    // ports
    Negative<LookupService> lookup = provides(LookupService.class);
    Negative<MaintenanceService> maintenance = provides(MaintenanceService.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    Positive<HerderService> herder = requires(HerderService.class);
    // finals
    private final long heartbeatInterval;
    private final long heartbeatTimeout;
    private final ReadWriteLock lutLock;
    // instance
    private GlobalInit initEvent;
    private Configuration config;
    private LookupTable lut;
    private Address self;
    private Integer selfId;
    private Key heartBeatKey;
    private Set<Address> masterGroup = null;
    private UUID sendHeartbeatId = null;
    private HashMap<Address, NodeStats> nodeStats = new HashMap<Address, NodeStats>();
    private TreeMap<Long, LUTUpdate> stalledUpdates = new TreeMap<Long, LUTUpdate>();
    private TreeMap<UUID, RangeQuery.SeqCollector> collectors = new TreeMap<UUID, RangeQuery.SeqCollector>();
    // Master
    private Positive<Store> masterStore = requires(Store.class);
    private Component catHerder = null;

    public LUTManager(GlobalInit init) {
        initEvent = init;
        config = init.conf;
        heartbeatInterval = config.getMilliseconds("caracal.heartbeatInterval");
        heartbeatTimeout = 2 * heartbeatInterval;
        lut = init.bootEvent.lut;
        lutLock = init.lock;
        self = init.self;
        selfId = lut.getIdsForAddresses(ImmutableSet.of(self)).get(self);
        heartBeatKey = LookupTable.RESERVED_HEARTBEATS.append(CustomSerialisers.serialiseAddress(self)).get();

        checkMasterGroup();

        subscribe(startHandler, control);
        subscribe(stopHandler, control);
        subscribe(lookupRHandler, lookup);
        subscribe(forwardHandler, lookup);
        subscribe(forwardToRangeHandler, lookup);
        subscribe(bootedHandler, maintenance);
        subscribe(forwardMsgHandler, net);
        subscribe(sampleHandler, net);
        subscribe(createSchemaHandler, net);
        subscribe(dropSchemaHandler, net);
        subscribe(statsHandler, maintenance);
        subscribe(sendHeartbeatHandler, timer);
        subscribe(maintenanceHandler, net);
        subscribe(rangeResponseHandler, net);
        subscribe(bootstrapHandler, net);
    }
    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.debug("{} starting initial nodes", self);
            /*
             * Timeouts
             */
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(0, heartbeatInterval);
            SendHeartbeat shb = new SendHeartbeat(spt);
            sendHeartbeatId = shb.getTimeoutId();
            spt.setTimeoutEvent(shb);
            trigger(spt, timer);
            if (checkMaster()) {
                startCatHerder();
            }
            /*
             * VNodes
             */
            startInitialVNodes();
        }
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            /*
             * Cleanup
             */
            if (sendHeartbeatId != null) {
                trigger(new CancelPeriodicTimeout(sendHeartbeatId), timer);
            }
        }
    };
    Handler<LookupRequest> lookupRHandler = new Handler<LookupRequest>() {
        @Override
        public void handle(LookupRequest event) {
            try {
                Address[] repGroup = lut.getResponsibles(event.key);
                LookupResponse rsp;
                if (repGroup == null) {
                    LOG.warn("No Node found reponsible for key {}!", event.key);
                    rsp = new LookupResponse(event, event.key, event.reqId, null);
                } else {
                    rsp = new LookupResponse(event, event.key, event.reqId, Arrays.asList(repGroup));
                }
                trigger(rsp, lookup);
            } catch (LookupTable.NoSuchSchemaException ex) {
                LOG.error("Can't get responsible node for key {}: {}", event.key, ex);
            }

        }
    };
    Handler<ForwardToAny> forwardHandler = new Handler<ForwardToAny>() {
        @Override
        public void handle(ForwardToAny event) {
            try {
                Address dest = lut.findDest(event.key, self, RAND);
                Msg msg = event.msg.insertDestination(self, dest, lut.versionId);
                trigger(msg, net);
                LOG.debug("{}: Forwarding {} to {}", new Object[]{self, event.msg, dest});
            } catch (LookupTable.NoResponsibleForKeyException ex) {
                LOG.warn("Dropping message!", ex);
            } catch (LookupTable.NoSuchSchemaException ex) {
                LOG.warn("Dropping message!", ex);
            }
            //TODO rewrite to check at destination and foward until reached right node
        }
    };
    Handler<ForwardToRange> forwardToRangeHandler = new Handler<ForwardToRange>() {

        @Override
        public void handle(ForwardToRange event) {
            if (event.execType.equals(RangeQuery.Type.SEQUENTIAL)) {
                Pair<KeyRange, Address[]> repGroup;
                try {
                    repGroup = lut.getFirstResponsibles(event.range);
                    if (repGroup == null) {
                        LOG.warn("No responsible nodes for range");
                        return;
                    }
                } catch (LookupTable.BrokenLut ex) {
                    LOG.error("Broken lut: {}", ex);
                    System.exit(1);
                    return;
                } catch (LookupTable.NoSuchSchemaException ex) {
                    LOG.error("Can't get responsible nodes for range {}: {}", event.range, ex);
                    return;
                }

                Address dest = null;

                // Try to deliver locally
                for (Address adr : repGroup.getValue1()) {
                    if (adr.sameHostAs(self)) {
                        dest = adr;
                    }
                }

                if (dest == null) {
                    //send to a random node of the replicationGroup
                    int nodePos = RAND.nextInt(repGroup.getValue1().length);
                    dest = repGroup.getValue1()[nodePos];
                }

                Msg msg = event.getSubRangeMessage(repGroup.getValue0(), self, dest);
                trigger(msg, net);
                LOG.debug("{}: Forwarding {} to {}", new Object[]{self, msg, dest});
            } else { //if(event.execType.equals(RangeQuery.Type.Parallel)
                NavigableMap<KeyRange, Address[]> repGroups;
                try {
                    repGroups = lut.getAllResponsibles(event.range);
                    if (repGroups.isEmpty()) {
                        LOG.warn("No responsible nodes for range");
                        return;
                    }
                } catch (LookupTable.BrokenLut ex) {
                    LOG.error("Broken lut: {}", ex);
                    System.exit(1);
                    return;
                } catch (LookupTable.NoSuchSchemaException ex) {
                    LOG.error("Can't get responsible nodes for range {}: {}", event.range, ex);
                    return;
                }
                for (Entry<KeyRange, Address[]> repGroup : repGroups.entrySet()) {
                    int nodePos = RAND.nextInt(repGroup.getValue().length);
                    Address dest = repGroup.getValue()[nodePos];
                    Msg msg = event.getSubRangeMessage(repGroup.getKey(), self, dest);
                    trigger(msg, net);
                    LOG.debug("{}: Forwarding {} to {}", new Object[]{self, msg, dest});
                }
            }
        }
    };
    Handler<ForwardMessage> forwardMsgHandler = new Handler<ForwardMessage>() {
        @Override
        public void handle(ForwardMessage event) {
            try {
                Address dest = lut.findDest(event.forwardTo, self, RAND);
                Msg msg = event.msg.insertDestination(self, dest, lut.versionId);
                trigger(msg, net);
                LOG.debug("{}: Forwarding {} to {}", new Object[]{self, event.msg, dest});
                if (event.msg.getLUTVersion() >= 0) { // has a lut version
                    if (event.msg.getLUTVersion() > lut.versionId) { // my LUT is outdated
                        
                    } else if (event.msg.getLUTVersion() < lut.versionId) { // their LUT is outdated
                        
                    }
                }
            } catch (LookupTable.NoResponsibleForKeyException ex) {
                LOG.warn("Dropping message!", ex);
            } catch (LookupTable.NoSuchSchemaException ex) {
                LOG.warn("Dropping message!", ex);
            }
        }
    };
    Handler<NodeBooted> bootedHandler = new Handler<NodeBooted>() {
        @Override
        public void handle(NodeBooted event) {
            Key nodeId = new Key(event.node.getId());
            try {
                View view = lut.getView(nodeId);
                KeyRange responsibility = lut.getResponsibility(nodeId);
                int quorum = view.members.size() / 2 + 1;
                NodeJoin join = new NodeJoin(view, quorum, responsibility, (view.id != 0));
                trigger(new MaintenanceMsg(self, event.node, join), net);
            } catch (LookupTable.NoSuchSchemaException ex) {
                LOG.error("{}: Couldn't find schema for node id {}: {}", new Object[]{self, nodeId, ex});
            }
        }
    };
    Handler<SendHeartbeat> sendHeartbeatHandler = new Handler<SendHeartbeat>() {

        @Override
        public void handle(SendHeartbeat event) {
            Report r = Stats.collect(self, nodeStats);
            try {
                PutRequest pr = new PutRequest(TimestampIdFactory.get().newId(), heartBeatKey, r.serialise());
                Address dest = lut.findDest(pr.key, self, RAND);
                CaracalMsg msg = new CaracalMsg(self, dest, pr);
                trigger(msg, net);
                LOG.debug("Sending Heartbeat: \n   {}\n   {}", r, pr);
            } catch (IOException ex) {
                LOG.error("Could not serialise Report. If this persists the node will be declared as dead due to lacking heartbeats.", ex);
            } catch (LookupTable.NoResponsibleForKeyException ex) {
                LOG.error("Dropping hearbeat. If this persists the node will be declared as dead due to lacking heartbeats", ex);
            } catch (LookupTable.NoSuchSchemaException ex) {
                LOG.error("Dropping hearbeat. If this persists the node will be declared as dead due to lacking heartbeats", ex);
            }
        }

    };
    Handler<NodeStats> statsHandler = new Handler<NodeStats>() {

        @Override
        public void handle(NodeStats event) {
            nodeStats.put(event.node, event);
        }
    };
    Handler<SampleRequest> sampleHandler = new Handler<SampleRequest>() {

        @Override
        public void handle(SampleRequest event) {
            ArrayList<Address> hosts = lut.hosts();
            if (hosts.size() <= event.n) {
                trigger(event.reply(ImmutableSet.copyOf(hosts), lut.schemas()), net);
                return;
            }
            Set<Address> sample = new TreeSet<Address>();
            for (int i = 0; i < event.n; i++) {
                Address addr = null;
                while (addr == null) {
                    addr = hosts.get(RAND.nextInt(hosts.size()));
                }
                sample.add(addr);
            }
            trigger(event.reply(ImmutableSet.copyOf(sample), lut.schemas()), net);
        }
    };
    Handler<Schema.CreateReq> createSchemaHandler = new Handler<Schema.CreateReq>() {

        @Override
        public void handle(Schema.CreateReq event) {
            LOG.trace("{}: Got Schema.CreateReq: {}", self, event);
            if (event.orig.equals(event.src)) { // hasn't been forwarded to masters, yet
                LOG.debug("{}: Forwarding Schema.CreateReq to masters: {}", self, masterGroup);
                for (Address addr : masterGroup) {
                    trigger(event.forward(self, addr), net);
                }
            } else { // master code
                if (!checkMaster()) {
                    LOG.warn("{}: Got a Schema.CreateReq forwarded although it's not master: {}", self, event);
                }
                // Do nothing...CatHerder will handle it
            }
        }
    };
    Handler<Schema.DropReq> dropSchemaHandler = new Handler<Schema.DropReq>() {

        @Override
        public void handle(Schema.DropReq event) {
            LOG.trace("{}: Got Schema.DropReq: {}", self, event);
            if (event.orig.equals(event.src)) { // hasn't been forwarded to masters, yet
                LOG.debug("{}: Forwarding Schema.DropReq to masters: {}", self, masterGroup);
                for (Address addr : masterGroup) {
                    trigger(event.forward(self, addr), net);
                }
            } else { // master code
                if (!checkMaster()) {
                    LOG.warn("{}: Got a Schema.DropReq forwarded although it's not master: {}", self, event);
                }
                // Do nothing...CatHerder will handle it
            }
        }
    };
    Handler<MaintenanceMsg> maintenanceHandler = new Handler<MaintenanceMsg>() {

        @Override
        public void handle(MaintenanceMsg event) {
            if (event.op instanceof LUTUpdated) {
                LUTUpdated updated = (LUTUpdated) event.op;
                LUTUpdate update = updated.update;
                LOG.info("{}: Got an update for the LUT: {}", self, update);
                if (!update.applicable(lut)) {
                    LOG.debug("{}: Deferring update. Current version {}, update version {}", new Object[]{self, lut.versionId, update.version});
                    stalledUpdates.put(update.version, update);
                    askForUpdatesTo(update.version);
                    return;
                }
                boolean masterBefore = checkMaster();
                ManagerCallbacks chc = new ManagerCallbacks();
                update.apply(lut, chc);
                chc.commit();
                LOG.info("{}: Applied LUTUpdate to version: {}", self, update.version);
                StringBuilder sb = new StringBuilder();
                lut.printFormat(sb);
                System.out.println("\n ***** NEW LUT ****** \n \n " + sb.toString());
                checkMasterGroup();
                if (checkMaster() && !masterBefore) {
                    startCatHerder();
                }
                if (masterBefore && !checkMaster()) {
                    LOG.error("{}: Just got demoted from master status. This is not supposed to happen! Failing hard...", self);
                    System.exit(1); // this might mean that the system is unstable, better to fail fast than risk further inconsistency
                }
                if (checkMaster()) {
                    trigger(new AppliedUpdate(update, event.src.equals(self)), herder);
                }
            }
        }
    };
    Handler<CaracalMsg> rangeResponseHandler = new Handler<CaracalMsg>() {

        @Override
        public void handle(CaracalMsg event) {
            if (event.op instanceof RangeQuery.Response) {
                RangeQuery.Response op = (RangeQuery.Response) event.op;
                RangeQuery.SeqCollector col = collectors.get(op.id);
                if ((col == null) || op.code != ResponseCode.SUCCESS) {
                    return; // nothing we can do without a collector
                }
                col.processResponse(op);
                if (col.isDone()) {
                    collectors.remove(op.id);
                    try {
                        boolean masterBefore = checkMaster();
                        for (Entry<Key, byte[]> e : col.getResult().getValue1().entrySet()) {
                            LUTUpdate update = LUTUpdate.deserialise(e.getValue());
                            if (!update.applicable(lut)) {
                                stalledUpdates.put(update.version, update);
                                askForUpdatesTo(update.version);
                                return;
                            }
                            ManagerCallbacks chc = new ManagerCallbacks();
                            update.apply(lut, chc);
                            chc.commit();
                            stalledUpdates.remove(update.version);
                        }
                        checkMasterGroup();
                        if (checkMaster() && !masterBefore) {
                            startCatHerder();
                        }
                        if (masterBefore && !checkMaster()) {
                            LOG.error("{}: Just got demoted from master status. This is not supposed to happen! Failing hard...", self);
                            System.exit(1); // this might mean that the system is unstable, better to fail fast than risk further inconsistency
                        }
                    } catch (Exception ex) {
                        LOG.error("{}: Error during LUTUpdate deserialisation: \n {}", self, ex);
                    }
                }
            }
        }

    };
    Handler<BootstrapRequest> bootstrapHandler = new Handler<BootstrapRequest>() {

        @Override
        public void handle(BootstrapRequest event) {
            if (event.origin.equals(event.src)) { // hasn't been forwarded to masters, yet
                for (Address addr : masterGroup) {
                    trigger(event.forward(self, addr), net);
                }
            } else { // master code
                if (!checkMaster()) {
                    LOG.warn("{}: Got a BoostrapRequest forwarded although it's not master: {}", self, event);
                }
                // Do nothing...CatHerder will handle it
            }
        }
    };

    private boolean checkMaster() {
        return masterGroup.contains(self);
    }

    private void checkMasterGroup() {
        masterGroup = new TreeSet<Address>();
        Address[] mGroup = lut.getHosts(0);
        masterGroup.addAll(Arrays.asList(mGroup));
    }

    private void startInitialVNodes() {
        Set<Key> localNodes = lut.getVirtualNodesAt(self);
        for (Key k : localNodes) {
            try {
                SchemaData.SingleSchema schema = lut.getSchema(k);
                if (schema == null) {
                    LOG.error("Could not find schema for key {}! Not starting VNode", k);
                    return;
                }
                trigger(new StartVNode(self, self, k.getArray(), schema), net);
            } catch (LookupTable.NoSuchSchemaException ex) {
                LOG.error("Could not find schema for key {}! Not starting VNode", k);
                return;
            }
        }
        LOG.debug("{}: Initial nodes are {}", self, localNodes);
    }

    private void startCatHerder() {
        catHerder = create(CatHerder.class, initEvent);
        connect(masterStore, catHerder.getNegative(Store.class));
        connect(net, catHerder.getNegative(Network.class));
        connect(timer, catHerder.getNegative(Timer.class));
        connect(herder.getPair(), catHerder.getPositive(HerderService.class));
        trigger(Start.event, catHerder.control());
    }

    class ManagerCallbacks implements LUTUpdate.Callbacks {

        private final TreeMap<Key, HostAction> actions = new TreeMap<Key, HostAction>();

        @Override
        public Address getAddress() {
            return self;
        }

        @Override
        public Integer getAddressId() {
            return selfId;
        }

        @Override
        public void killVNode(Key k) {
            HostAction cur = actions.get(k);
            if (cur == null) {
                actions.put(k, this.new Kill());
                return;
            }
            if (cur instanceof ManagerCallbacks.Kill) {
                return; // no reason to double kill
            }
            if (cur instanceof ManagerCallbacks.Start) {
                actions.remove(k); // one start and one kill cancel each other out
                return;
            }
            if (cur instanceof ManagerCallbacks.Reconf) {
                LOG.error("{}: Got a kill action at {} where there's currently a reconf. Not sure what to do -.-", self, k);
                actions.remove(k); // better do nothing than do weird things
                return;
            }
        }

        @Override
        public void startVNode(Key k) {
            HostAction cur = actions.get(k);
            if (cur == null) {
                actions.put(k, this.new Start());
                return;
            }
            if (cur instanceof ManagerCallbacks.Kill) {
                actions.remove(k); // one start and one kill cancel each other out
                return;
            }
            if (cur instanceof ManagerCallbacks.Start) {
                return; // no reason to double start
            }
            if (cur instanceof ManagerCallbacks.Reconf) {
                LOG.error("{}: Got a start action at {} where there's currently a reconf. Not sure what to do -.-", self, k);
                actions.remove(k); // better do nothing than do weird things
                return;
            }
        }

        @Override
        public void reconf(Key k, View v, int quorum, KeyRange range) {
            ViewChange change = new ViewChange(v, quorum, range);
            HostAction cur = actions.get(k);
            if (cur == null) {
                actions.put(k, this.new Reconf(change));
                return;
            }
            if (cur instanceof ManagerCallbacks.Kill) {
                LOG.error("{}: Got a reconf action at {} where there's currently a kill. Not sure what to do -.-", self, k);
                actions.remove(k); // better do nothing than do weird things
                return;
            }
            if (cur instanceof ManagerCallbacks.Start) {
                LOG.error("{}: Got a reconf action at {} where there's currently a start. Not sure what to do -.-", self, k);
                actions.remove(k); // better do nothing than do weird things
                return;
            }
            if (cur instanceof ManagerCallbacks.Reconf) {
                ManagerCallbacks.Reconf re = (ManagerCallbacks.Reconf) cur;
                if (re.change.equals(change)) {
                    return; // it's fine
                }
                LOG.error("{}: Got a different reconf action at {} where there's currently a reconf. Not sure what to do -.-", self, k);
                actions.remove(k); // better do nothing than do weird things
                return;
            }
        }

        public void commit() {
            for (Entry<Key, HostAction> e : actions.entrySet()) {
                e.getValue().execute(e.getKey());
            }
        }

        class Kill extends HostAction {

            @Override
            public void execute(Key k) {
                StopVNode msg = new StopVNode(self, self.newVirtual(k.getArray()));
                trigger(msg, net);
            }

        }

        class Start extends HostAction {

            @Override
            public void execute(Key k) {
                try {
                    SchemaData.SingleSchema schema = lut.getSchema(k);
                    if (schema == null) {
                        LOG.error("Could not find schema for key {}! Not starting VNode", k);
                        return;
                    }
                    StartVNode msg = new StartVNode(self, self, k.getArray(), schema);
                    trigger(msg, net);
                } catch (LookupTable.NoSuchSchemaException ex) {
                    LOG.error("Could not find schema for key {}! Not starting VNode", k);
                    return;
                }
            }

        }

        class Reconf extends HostAction {

            public final ViewChange change;

            public Reconf(ViewChange change) {
                this.change = change;
            }

            @Override
            public void execute(Key k) {
                Reconfiguration r = new Reconfiguration(change);
                MaintenanceMsg msg = new MaintenanceMsg(self, self.newVirtual(k.getArray()), r);
                trigger(msg, net);
            }
        }

    }

    static abstract class HostAction {

        public abstract void execute(Key k);
    }

    private void askForUpdatesTo(long version) {
        try {
            Key startKey = LookupTable.RESERVED_LUTUPDATES.append(new Key(Longs.toByteArray(lut.versionId))).get();
            Key endKey = LookupTable.RESERVED_LUTUPDATES.append(new Key(Longs.toByteArray(version))).get();
            KeyRange range = KeyRange.open(startKey).open(endKey);
            UUID id = TimestampIdFactory.get().newId();
            RangeQuery.Request r = new RangeQuery.Request(id, range, null, null, null, RangeQuery.Type.SEQUENTIAL);
            Address dest = lut.findDest(startKey, self, RAND);
            CaracalMsg msg = new CaracalMsg(self, dest, r);
            trigger(msg, net);
            collectors.put(id, new RangeQuery.SeqCollector(r));
        } catch (LookupTable.NoResponsibleForKeyException ex) {
            LOG.error("{}: Apparently noone is responsible for the reserved range -.-: {}", self, ex);
        } catch (LookupTable.NoSuchSchemaException ex) {
            LOG.error("{}: Apparently the reserved range doesn't have a schema!!! -.-: {}", self, ex);
        }
    }

    public static class SendHeartbeat extends Timeout {

        public SendHeartbeat(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }

}
