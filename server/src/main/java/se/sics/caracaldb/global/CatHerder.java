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
import com.google.common.primitives.Longs;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.bootstrap.BootUp;
import se.sics.caracaldb.bootstrap.BootstrapRequest;
import se.sics.caracaldb.bootstrap.BootstrapResponse;
import se.sics.caracaldb.bootstrap.Ready;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.MultiOpRequest;
import se.sics.caracaldb.operations.MultiOpResponse;
import se.sics.caracaldb.operations.OpUtil;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.store.ActionFactory;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.RangeReq;
import se.sics.caracaldb.store.RangeResp;
import se.sics.caracaldb.store.Store;
import se.sics.caracaldb.store.TFFactory;
import se.sics.caracaldb.system.Stats;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author lkroll
 */
public class CatHerder extends ComponentDefinition {

    // statics
    private static final Logger LOG = LoggerFactory.getLogger(CatHerder.class);
    private static final Random RAND = new Random();
    // ports
    Negative<HerderService> herder = provides(HerderService.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    Positive<Store> store = requires(Store.class);
    // finals
    private final long heartbeatInterval;
    private final long heartbeatTimeout;
    private final ReadWriteLock lutLock;
    // instance
    private LookupTable lut; // READ-ONLY!
    private Address self;
    private UUID checkHeartbeatsId = null;

    // master
    private HashSet<Address> outstandingJoins = new HashSet<Address>();
    private HashSet<Schema.Req> outstandingSchemaChanges = new HashSet<Schema.Req>();
    private TreeMap<UUID, LUTUpdate> pendingUpdates = new TreeMap<UUID, LUTUpdate>();
    //private Component paxos;
    //private Positive<ReplicatedLog> rlog = requires(ReplicatedLog.class);
    private final MaintenancePolicy policy;

    public CatHerder(GlobalInit init) {
        heartbeatInterval = init.conf.getMilliseconds("caracal.heartbeatInterval");
        heartbeatTimeout = 2 * heartbeatInterval;
        policy = init.conf.getMaintenancePolicy();
        lutLock = init.lock;
        lut = init.bootEvent.lut;
        self = init.self;
        policy.init(init.conf.getIdGenerator());

        // Subscriptions
        subscribe(startHandler, control);
        subscribe(stopHandler, control);
        subscribe(checkHBHandler, timer);
        subscribe(rangeHandler, store);
        subscribe(multiOpHandler, net);
        subscribe(readyHandler, net);
        subscribe(createSchemaHandler, net);
        subscribe(dropSchemaHandler, net);
        subscribe(bootstrapHandler, net);
        subscribe(updateHandler, herder);

        scheduleTimeout();
    }

    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            scheduleTimeout();
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
        }
    };

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
            LOG.info("{}: Got range response with {} items!", self, event.result.size());
            ImmutableMap.Builder<Address, Stats.Report> statsB = ImmutableMap.builder();
            for (Map.Entry<Key, byte[]> e : event.result.entrySet()) {
                Key k = e.getKey();
                try {
                    Stats.Report r = Stats.Report.deserialise(e.getValue());
                    statsB.put(r.atHost, r);
                } catch (IOException ex) {
                    LOG.error("Could not deserialise Statistics Report for key " + k, ex);
                }
            }
            lutLock.readLock().lock();
            try {
                ImmutableMap<Address, Stats.Report> stats = statsB.build();
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
                LUTWorkingBuffer buffer = new LUTWorkingBuffer(lut);
                LOG.debug("Running rebalancer with {} outstanding joins, {} fails and {} schema changes.", new Object[]{outstandingJoins.size(), fails.size(), outstandingSchemaChanges.size()});
                policy.rebalance(buffer, fails, ImmutableSet.copyOf(outstandingJoins), stats, ImmutableSet.copyOf(outstandingSchemaChanges));
                LUTUpdate update = buffer.assembleUpdate();
                if (update == null) {
                    LOG.debug("{}: No new LUT version created by policy.", self);
                    return; // nothing to do
                }
                LOG.info("{}: Created new LUTUpdate: {}", self, update);
                try {
                    Key updateKey = LookupTable.RESERVED_LUTUPDATES.append(new Key(Longs.toByteArray(update.version))).get();
                    UUID id = TimestampIdFactory.get().newId();
                    MultiOpRequest op = OpUtil.putIfAbsent(id, updateKey, update.serialise());
                    Address dest = lut.findDest(updateKey, self, RAND);
                    CaracalMsg msg = new CaracalMsg(self, dest, op);
                    trigger(msg, net);
                    pendingUpdates.put(id, update);
                } catch (LookupTable.NoResponsibleForKeyException ex) {
                    LOG.error("{}: Apparently no-one is responsible for the reserved range oO: {}", self, ex);
                } catch (LookupTable.NoSuchSchemaException ex) {
                    LOG.error("{}: Apparently there is not schema for the reserved range oO: {}", self, ex);
                    return;
                }
                outstandingJoins.clear(); // clear occasionally to keep only active nodes in there
            } finally {
                lutLock.readLock().unlock();
            }
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
                    LOG.info("{}: My new LUT version {} got accepted. Broadcasting!", self, update.version);
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
    Handler<Schema.CreateReq> createSchemaHandler = new Handler<Schema.CreateReq>() {

        @Override
        public void handle(Schema.CreateReq event) {
            if (!event.orig.equals(event.src)) {
                LOG.info("{}: Taking note of pending Schema.CreateReq: {}", self, event);
                // TODO check if there needs to be some check about existance of nodes in the LUT here
                outstandingSchemaChanges.add(event);
            }
            // Ignore, since it will be forwarded again (just to avoid duplication)
        }
    };
    Handler<Schema.DropReq> dropSchemaHandler = new Handler<Schema.DropReq>() {

        @Override
        public void handle(Schema.DropReq event) {
            if (!event.orig.equals(event.src)) {
                LOG.info("{}: Taking note of pending Schema.DropReq: {}", self, event);
                // TODO check if there needs to be some check about existance of nodes in the LUT here
                outstandingSchemaChanges.add(event);
            }
            // Ignore, since it will be forwarded again (just to avoid duplication)
        }
    };
    Handler<BootstrapRequest> bootstrapHandler = new Handler<BootstrapRequest>() {

        @Override
        public void handle(BootstrapRequest event) {
            if (!event.origin.equals(event.src)) { // hasn't been forwarded to masters, yet
                // TODO check if there needs to be some check about existance of nodes in the LUT here
                outstandingJoins.add(event.origin);
            }
            // Ignore, since it will be forwarded again (just to avoid duplication)
        }
    };
    Handler<AppliedUpdate> updateHandler = new Handler<AppliedUpdate>() {

        @Override
        public void handle(AppliedUpdate event) {
            LOG.debug("{}: Got AppliedUpdate event...cleaning up local state...", self);
            lutLock.readLock().lock();
            HashSet<Schema.Req> addback = new HashSet<Schema.Req>();
            try {
                for (Schema.Req req : outstandingSchemaChanges) {
                    if (req instanceof Schema.CreateReq) {
                        Schema.CreateReq creq = (Schema.CreateReq) req;
                        ByteBuffer schemaId = lut.schemas().schemaIDs.get(creq.name);
                        if (schemaId == null) {
                            addback.add(req);
                            continue;
                        }
                        if (event.isOwnUpdate) {
                            Schema.Response res = creq.reply(self, schemaId.array());
                            trigger(res, net);
                        }
                    } else if (req instanceof Schema.DropReq) {
                        Schema.DropReq dreq = (Schema.DropReq) req;
                        ByteBuffer schemaId = lut.schemas().schemaIDs.get(dreq.name);
                        if (schemaId != null) {
                            addback.add(req);
                            continue;
                        }
                        if (event.isOwnUpdate) {
                            Schema.Response res = dreq.reply(self, null);
                            trigger(res, net);
                        }
                    }
                }
                outstandingSchemaChanges.clear();
                outstandingSchemaChanges.addAll(addback);
            } finally {
                lutLock.readLock().unlock();
            }
            bootstrapJoiners(event.update);
        }
    };

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

    private void scheduleTimeout() {
        SchedulePeriodicTimeout sptC = new SchedulePeriodicTimeout(heartbeatTimeout, heartbeatTimeout);
        LOG.info("{}: Scheduling Heartbeats: {}ms < {}ms", self, heartbeatInterval, heartbeatTimeout);
        CheckHeartbeats chs = new CheckHeartbeats(sptC);
        checkHeartbeatsId = chs.getTimeoutId();
        sptC.setTimeoutEvent(chs);
        trigger(sptC, timer);
    }

    public static class CheckHeartbeats extends Timeout {

        public CheckHeartbeats(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
}
