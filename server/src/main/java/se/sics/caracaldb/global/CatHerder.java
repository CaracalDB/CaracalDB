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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.bootstrap.BootUp;
import se.sics.caracaldb.bootstrap.BootstrapRequest;
import se.sics.caracaldb.bootstrap.BootstrapResponse;
import se.sics.caracaldb.bootstrap.Ready;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.MultiOpRequest;
import se.sics.caracaldb.operations.MultiOpResponse;
import se.sics.caracaldb.operations.OpUtil;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.replication.linearisable.ViewChange;
import se.sics.caracaldb.store.ActionFactory;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.RangeReq;
import se.sics.caracaldb.store.RangeResp;
import se.sics.caracaldb.store.Store;
import se.sics.caracaldb.store.TFFactory;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.StartVNode;
import se.sics.caracaldb.system.Stats;
import se.sics.caracaldb.system.Stats.Report;
import se.sics.caracaldb.system.StopVNode;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.caracaldb.utils.TimestampIdFactory;
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
public class CatHerder extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(CatHerder.class);
    private static final Random RAND = new Random();
    // ports
    Negative<LookupService> lookup = provides(LookupService.class);
    Negative<MaintenanceService> maintenance = provides(MaintenanceService.class);
    Positive<Network> net = requires(Network.class);
    Positive<EventualFailureDetector> fd = requires(EventualFailureDetector.class);
    Positive<Timer> timer = requires(Timer.class);
    Positive<Store> store = requires(Store.class);
    // finals
    private final long heartbeatInterval;
    private final long heartbeatTimeout;
    // instance
    private Configuration config;
    private LookupTable lut;
    private Address self;
    private Integer selfId;
    private Key heartBeatKey;
    private Set<Address> masterGroup = null;
    private UUID sendHeartbeatId = null;
    private UUID checkHeartbeatsId = null;
    private HashMap<Address, NodeStats> nodeStats = new HashMap<Address, NodeStats>();
    private TreeMap<Long, LUTUpdate> stalledUpdates = new TreeMap<Long, LUTUpdate>();
    private TreeMap<UUID, RangeQuery.SeqCollector> collectors = new TreeMap<UUID, RangeQuery.SeqCollector>();
    // master
    private HashSet<Address> outstandingJoins = new HashSet<Address>();
    private TreeMap<UUID, LUTUpdate> pendingUpdates = new TreeMap<UUID, LUTUpdate>();
    //private Component paxos;
    //private Positive<ReplicatedLog> rlog = requires(ReplicatedLog.class);
    private final MaintenancePolicy policy;

    public CatHerder(CatHerderInit init) {
        config = init.conf;
        heartbeatInterval = config.getMilliseconds("caracal.heartbeatInterval");
        heartbeatTimeout = 2 * heartbeatInterval;
        lut = init.bootEvent.lut;
        self = init.self;
        selfId = lut.getIdsForAddresses(ImmutableSet.of(self)).get(self);
        heartBeatKey = LookupTable.RESERVED_HEARTBEATS.append(CustomSerialisers.serialiseAddress(self)).get();

        policy = init.conf.getMaintenancePolicy();
        policy.init(lut);

        checkMasterGroup();
        if (checkMaster()) {
            connectMasterHandlers();
        } else {
            connectSlaveHandlers();
        }
        subscribe(startHandler, control);
        subscribe(stopHandler, control);
        subscribe(lookupRHandler, lookup);
        subscribe(forwardHandler, lookup);
        subscribe(forwardToRangeHandler, lookup);
        subscribe(bootedHandler, maintenance);
        subscribe(forwardMsgHandler, net);
        subscribe(sampleHandler, net);
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
                scheduleMasterTimeout();
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
            if (checkHeartbeatsId != null) {
                trigger(new CancelPeriodicTimeout(checkHeartbeatsId), timer);
            }
            if (sendHeartbeatId != null) {
                trigger(new CancelPeriodicTimeout(sendHeartbeatId), timer);
            }
        }
    };
    Handler<LookupRequest> lookupRHandler = new Handler<LookupRequest>() {
        @Override
        public void handle(LookupRequest event) {
            Address[] repGroup = lut.getResponsibles(event.key);
            LookupResponse rsp;
            if (repGroup == null) {
                LOG.warn("No Node found reponsible for key {}!", event.key);
                rsp = new LookupResponse(event, event.key, event.reqId, null);
            } else {
                rsp = new LookupResponse(event, event.key, event.reqId, Arrays.asList(repGroup));
            }
            trigger(rsp, lookup);

        }
    };
    Handler<ForwardToAny> forwardHandler = new Handler<ForwardToAny>() {
        @Override
        public void handle(ForwardToAny event) {
            try {
                Address dest = findDest(event.key);
                Msg msg = event.msg.insertDestination(self, dest);
                trigger(msg, net);
                LOG.debug("{}: Forwarding {} to {}", new Object[]{self, event.msg, dest});
            } catch (NoResponsibleForKeyException ex) {
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
                    LOG.error("Broken lut");
                    System.exit(1);
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
                    LOG.error("Broken lut");
                    System.exit(1);
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
                Address dest = findDest(event.forwardTo);
                Msg msg = event.msg.insertDestination(self, dest);
                trigger(msg, net);
                LOG.debug("{}: Forwarding {} to {}", new Object[]{self, event.msg, dest});
            } catch (NoResponsibleForKeyException ex) {
                LOG.warn("Dropping message!", ex);
            }
        }
    };
    Handler<NodeBooted> bootedHandler = new Handler<NodeBooted>() {
        @Override
        public void handle(NodeBooted event) {
            Key nodeId = new Key(event.node.getId());
            View view = lut.getView(nodeId);
            KeyRange responsibility = lut.getResponsibility(nodeId);
            int quorum = view.members.size() / 2 + 1;
            NodeJoin join = new NodeJoin(view, quorum, responsibility, (view.id != 0));
            trigger(new MaintenanceMsg(self, event.node, join), net);
        }
    };
    Handler<SendHeartbeat> sendHeartbeatHandler = new Handler<SendHeartbeat>() {

        @Override
        public void handle(SendHeartbeat event) {
            Report r = Stats.collect(self, nodeStats);
            try {
                PutRequest pr = new PutRequest(TimestampIdFactory.get().newId(), heartBeatKey, r.serialise());
                Address dest = findDest(pr.key);
                CaracalMsg msg = new CaracalMsg(self, dest, pr);
                trigger(msg, net);
                LOG.debug("Sending Heartbeat: \n   {}\n   {}", r, pr);
            } catch (IOException ex) {
                LOG.error("Could not serialise Report. If this persists the node will be declared as dead due to lacking heartbeats.", ex);
            } catch (NoResponsibleForKeyException ex) {
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
                trigger(event.reply(ImmutableSet.copyOf(hosts)), net);
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
            trigger(event.reply(ImmutableSet.copyOf(sample)), net);
        }
    };
    Handler<MaintenanceMsg> maintenanceHandler = new Handler<MaintenanceMsg>() {

        @Override
        public void handle(MaintenanceMsg event) {
            if (event.op instanceof LUTUpdate) {
                LUTUpdate update = (LUTUpdate) event.op;
                if (!update.applicable(lut)) {
                    stalledUpdates.put(update.version, update);
                    askForUpdatesTo(update.version);
                    return;
                }
                boolean masterBefore = checkMaster();
                CatHerderCallbacks chc = new CatHerderCallbacks();
                update.apply(lut, chc);
                chc.commit();
                checkMasterGroup();
                if (checkMaster() && !masterBefore) {
                    connectMasterHandlers();
                    scheduleMasterTimeout();
                }
                if (masterBefore && !checkMaster()) {
                    LOG.error("{}: Just got demoted from master status. This is not supposed to happen! Failing hard...", self);
                    System.exit(1); // this might mean that the system is unstable, better to fail fast than risk further inconsistency
                }
                if (event.src.equals(self)) { // a tiny bit of master code
                    bootstrapJoiners(update);
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
                            CatHerderCallbacks chc = new CatHerderCallbacks();
                            update.apply(lut, chc);
                            chc.commit();
                            stalledUpdates.remove(update.version);
                        }
                        checkMasterGroup();
                        if (checkMaster() && !masterBefore) {
                            connectMasterHandlers();
                            scheduleMasterTimeout();
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
                    return;
                }
                // TODO check if there needs to be some check about existance of nodes in the LUT here
                outstandingJoins.add(event.origin);
            }
        }
    };

    class CatHerderCallbacks implements LUTUpdate.Callbacks {

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
            if (cur instanceof CatHerderCallbacks.Kill) {
                return; // no reason to double kill
            }
            if (cur instanceof CatHerderCallbacks.Start) {
                actions.remove(k); // one start and one kill cancel each other out
                return;
            }
            if (cur instanceof CatHerderCallbacks.Reconf) {
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
            if (cur instanceof CatHerderCallbacks.Kill) {
                actions.remove(k); // one start and one kill cancel each other out
                return;
            }
            if (cur instanceof CatHerderCallbacks.Start) {
                return; // no reason to double start
            }
            if (cur instanceof CatHerderCallbacks.Reconf) {
                LOG.error("{}: Got a start action at {} where there's currently a reconf. Not sure what to do -.-", self, k);
                actions.remove(k); // better do nothing than do weird things
                return;
            }
        }

        @Override
        public void reconf(Key k, ViewChange change) {
            HostAction cur = actions.get(k);
            if (cur == null) {
                actions.put(k, this.new Reconf(change));
                return;
            }
            if (cur instanceof CatHerderCallbacks.Kill) {
                LOG.error("{}: Got a reconf action at {} where there's currently a kill. Not sure what to do -.-", self, k);
                actions.remove(k); // better do nothing than do weird things
                return;
            }
            if (cur instanceof CatHerderCallbacks.Start) {
                LOG.error("{}: Got a reconf action at {} where there's currently a start. Not sure what to do -.-", self, k);
                actions.remove(k); // better do nothing than do weird things
                return;
            }
            if (cur instanceof CatHerderCallbacks.Reconf) {
                CatHerderCallbacks.Reconf re = (CatHerderCallbacks.Reconf) cur;
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
                StartVNode msg = new StartVNode(self, self, k.getArray());
                trigger(msg, net);
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
            Address dest = findDest(startKey);
            CaracalMsg msg = new CaracalMsg(self, dest, r);
            trigger(msg, net);
            collectors.put(id, new RangeQuery.SeqCollector(r));
        } catch (NoResponsibleForKeyException ex) {
            LOG.warn("{}: Apparently noone is responsible for the reserved range -.-", self);
        }
    }

    /*
     * MASTER ONLY
     */
    Handler<CheckHeartbeats> checkHBHandler = new Handler<CheckHeartbeats>() {

        @Override
        public void handle(CheckHeartbeats event) {
            RangeReq rr = new RangeReq(KeyRange.prefix(LookupTable.RESERVED_HEARTBEATS), Limit.noLimit(), TFFactory.noTF(), ActionFactory.fullDelete(), -1);
            /*
             * I suppose one can argue whether or not it's correct to do this
             * directly on the storage, bypassing the replication. But consider
             * this: If you involve the replication, then there is really no
             * benefit to collocating the reserved range with the master group.
             */
            trigger(rr, store);
        }
    };
    Handler<RangeResp> rangeHandler = new Handler<RangeResp>() {

        @Override
        public void handle(RangeResp event) {
            LOG.info("{}({}): Got range response with {} items!", self, checkMaster(), event.result.size());
            ImmutableMap.Builder<Address, Report> statsB = ImmutableMap.builder();
            for (Entry<Key, byte[]> e : event.result.entrySet()) {
                Key k = e.getKey();
                try {
                    Report r = Report.deserialise(e.getValue());
                    statsB.put(r.atHost, r);
                } catch (IOException ex) {
                    LOG.error("Could not deserialise Statistics Report for key " + k, ex);
                }
            }
            ImmutableMap<Address, Report> stats = statsB.build();
            ImmutableSet.Builder<Address> failsB = ImmutableSet.builder();
            for (Iterator<Address> it = lut.hostIterator(); it.hasNext();) {
                Address host = it.next();
                if ((host != null) && !stats.containsKey(host)) {
                    failsB.add(host);
                    LOG.info("{}: It appears that host {} has failed.", self, host);
                }
            }
            ImmutableSet<Address> fails = failsB.build();
            if (fails.size() > (lut.hosts().size() / 2)) {
                LOG.warn("{}: More than 50% of all system nodes seem to have failed at the same time. \n"
                        + "Under the assumption that this is a partition Caracal will not rebalance, but wait for the partition to be resolved.", self);
                return;
            }
            LUTUpdate update = policy.rebalance(fails, ImmutableSet.copyOf(outstandingJoins), stats);
            if (update == null) {
                return; // nothing to do
            }
            try {
                Key updateKey = LookupTable.RESERVED_LUTUPDATES.append(new Key(Longs.toByteArray(update.version))).get();
                UUID id = TimestampIdFactory.get().newId();
                MultiOpRequest op = OpUtil.putIfAbsent(id, updateKey, update.serialise());
                Address dest = findDest(updateKey);
                CaracalMsg msg = new CaracalMsg(self, dest, op);
                trigger(msg, net);
                pendingUpdates.put(id, update);
            } catch (NoResponsibleForKeyException ex) {
                LOG.error("{}: Apparently no-one is responsible for the reserved range oO: {}", self, ex);
            }
            outstandingJoins.clear(); // clear occasionally to keep only active nodes in there
        }
    };
    Handler<CaracalMsg> multiOpHandler = new Handler<CaracalMsg>() {

        @Override
        public void handle(CaracalMsg event) {
            if (event.op instanceof MultiOpResponse) {
                MultiOpResponse mor = (MultiOpResponse) event.op;
                LUTUpdate update = pendingUpdates.remove(mor.id);
                if (update == null) {
                    LOG.info("{}: Can't find pending update with id {}", self, mor.id);
                    return;
                }
                if (mor.code == ResponseCode.SUCCESS && mor.success == true) {
                    broadcast(update);
                }
                return;
            }
            //LOG.info("{}: Got CaracalMsg {}, but am only expecting MultiOpResponse", self, event);
        }

    };
    Handler<Ready> readyHandler = new Handler<Ready>() {

        @Override
        public void handle(Ready event) {
            BootUp bu = new BootUp(self, event.getOrigin());
            trigger(bu, net); // no need to wait for anyone
        }
    };

    private Address findDest(Key k) throws NoResponsibleForKeyException {
        Address[] repGroup = lut.getResponsibles(k);
        if (repGroup == null) {
            throw new NoResponsibleForKeyException(k);
        }
        // Try to deliver locally
        for (Address adr : repGroup) {
            if (adr.sameHostAs(self)) {
                return adr;
            }
        }
        // Otherwise just pick at random
        int nodePos = RAND.nextInt(repGroup.length);
        Address dest = repGroup[nodePos];
        return dest;
    }

    private void broadcast(Maintenance op) {
        for (Address addr : lut.hosts()) {
            MaintenanceMsg msg = new MaintenanceMsg(self, addr, op);
            trigger(msg, net);
        }
    }

    private void bootstrapJoiners(LUTUpdate update) {
        ImmutableSet<Address> joiners = update.joiners();
        if (joiners.isEmpty()) {
            return;
        }
        byte[] lutS = lut.serialise();
        for (Address addr : joiners) {
            BootstrapResponse br = new BootstrapResponse(self, addr, lutS);
            trigger(br, net);
            outstandingJoins.remove(addr);
        }
    }

    private void connectMasterHandlers() {
        View v = new View(ImmutableSortedSet.copyOf(masterGroup), 0);
//        paxos = create(Paxos.class,
//                new PaxosInit(v, (int) Math.ceil(LookupTable.INIT_REP_FACTOR / 2.0),
//                        config.getMilliseconds("caracal.network.keepAlivePeriod"), self));
//        connect(rlog.getPair(), paxos.getPositive(ReplicatedLog.class));
//        connect(paxos.getNegative(Network.class), net);
//        connect(paxos.getNegative(EventualFailureDetector.class), fd);
//
//        subscribe(decideHandler, rlog);
        subscribe(checkHBHandler, timer);
        subscribe(rangeHandler, store);
        subscribe(multiOpHandler, net);
        subscribe(readyHandler, net);
    }

    private void scheduleMasterTimeout() {
        SchedulePeriodicTimeout sptC = new SchedulePeriodicTimeout(heartbeatTimeout, heartbeatTimeout);
        LOG.info("{}: Scheduling Heartbeats: {}ms < {}ms", self, heartbeatInterval, heartbeatTimeout);
        CheckHeartbeats chs = new CheckHeartbeats(sptC);
        checkHeartbeatsId = chs.getTimeoutId();
        sptC.setTimeoutEvent(chs);
        trigger(sptC, timer);
    }

    private void connectSlaveHandlers() {

    }

    private void startInitialVNodes() {
        Set<Key> localNodes = lut.getVirtualNodesAt(self);
        for (Key k : localNodes) {
            trigger(new StartVNode(self, self, k.getArray()), net);
        }
        LOG.debug("{}: Initial nodes are {}", self, localNodes);
    }

    private boolean checkMaster() {
        return masterGroup.contains(self);
    }

    private void checkMasterGroup() {
        masterGroup = new TreeSet<Address>();
        Address[] mGroup = lut.getHosts(0);
        masterGroup.addAll(Arrays.asList(mGroup));
    }

    public static class SendHeartbeat extends Timeout {

        public SendHeartbeat(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }

    public static class CheckHeartbeats extends Timeout {

        public CheckHeartbeats(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }

    public static class NoResponsibleForKeyException extends Exception {

        public final Key key;

        public NoResponsibleForKeyException(Key k) {
            key = k;
        }

        @Override
        public String getMessage() {
            return "No Node found reponsible for key " + key;
        }

    }
}
