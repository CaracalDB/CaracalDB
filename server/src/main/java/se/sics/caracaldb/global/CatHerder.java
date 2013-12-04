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

import com.google.common.collect.ImmutableSortedSet;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.paxos.Paxos;
import se.sics.caracaldb.paxos.PaxosInit;
import se.sics.caracaldb.replication.log.Decide;
import se.sics.caracaldb.replication.log.ReplicatedLog;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.RangeReq;
import se.sics.caracaldb.store.RangeResp;
import se.sics.caracaldb.store.Store;
import se.sics.caracaldb.store.TFFactory;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.StartVNode;
import se.sics.caracaldb.system.Stats;
import se.sics.caracaldb.system.Stats.Report;
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
import se.sics.kompics.network.Message;
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
    private Key heartBeatKey;
    private Set<Address> masterGroup = null;
    private UUID sendHeartbeatId = null;
    private UUID checkHeartbeatsId = null;
    private HashMap<Address, NodeStats> nodeStats = new HashMap<Address, NodeStats>();
    // master
    private HashSet<Address> outstandingFailures = new HashSet<Address>();
    private Component paxos;
    private Positive<ReplicatedLog> rlog = requires(ReplicatedLog.class);

    public CatHerder(CatHerderInit init) {
        config = init.conf;
        heartbeatInterval = config.getMilliseconds("caracal.heartbeatInterval");
        heartbeatTimeout = 2 * heartbeatInterval;
        lut = init.bootEvent.lut;
        self = init.self;
        try {
            heartBeatKey = LookupTable.RESERVED_HEARTBEATS.append(CustomSerialisers.serialiseAddress(self)).get();
        } catch (IOException ex) {
            throw new RuntimeException(ex); // No idea what to do if this doesn't work.
        }
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
        subscribe(statsHandler, maintenance);
        subscribe(sendHeartbeatHandler, timer);
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
                SchedulePeriodicTimeout sptC = new SchedulePeriodicTimeout(heartbeatTimeout, heartbeatTimeout);
                CheckHeartbeats chs = new CheckHeartbeats(sptC);
                checkHeartbeatsId = chs.getTimeoutId();
                sptC.setTimeoutEvent(chs);
                trigger(sptC, timer);
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
                Message msg = event.msg.insertDestination(dest);
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

                Message msg = event.getSubRangeMessage(repGroup.getValue0(), dest);
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
                    Message msg = event.getSubRangeMessage(repGroup.getKey(), dest);
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
                Message msg = event.msg.insertDestination(dest);
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
                LOG.debug("Sending Heartbeat: {}", r);
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
    /*
     * MASTER ONLY
     */
    Handler<CheckHeartbeats> checkHBHandler = new Handler<CheckHeartbeats>() {

        private RangeReq rr = null;

        @Override
        public void handle(CheckHeartbeats event) {
            if (rr == null) {
                rr = new RangeReq(KeyRange.prefix(heartBeatKey), Limit.noLimit(), TFFactory.noTF());
            }
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
            HashMap<Address, Report> stats = new HashMap<Address, Report>();
            for (Entry<Key, byte[]> e : event.result.entrySet()) {
                Key k = e.getKey();
                try {
                    Report r = Report.deserialise(e.getValue());
                    stats.put(r.atHost, r);
                } catch (IOException ex) {
                    LOG.error("Could not deserialise Statistics Report for key " + k, ex);
                }
            }
            HashSet<Address> fails = new HashSet<Address>();
            for (Iterator<Address> it = lut.hostIterator(); it.hasNext();) {
                Address host = it.next();
                if ((host != null) && !stats.containsKey(host)) {
                    fails.add(host);
                    LOG.info("{}: It appears that host {} has failed.", self, host);
                }
            }
            // Can already be deallocated while failures are being handled
            // Somtimes I wish I could just tell the JVM GC what to do -.-
            stats.clear();
            stats = null;
            for (Address adr : fails) {
                //TODO something
            }
        }
    };
    Handler<Decide> decideHandler = new Handler<Decide>() {

        @Override
        public void handle(Decide event) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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

    private void announceFailure(Address failedHost) {

    }

    private void connectMasterHandlers() {
        View v = new View(ImmutableSortedSet.copyOf(masterGroup), 0);
        paxos = create(Paxos.class,
                new PaxosInit(v, (int) Math.ceil(LookupTable.INIT_REP_FACTOR / 2.0),
                        config.getMilliseconds("caracal.network.keepAlivePeriod"), self));
        connect(rlog.getPair(), paxos.getPositive(ReplicatedLog.class));
        connect(paxos.getNegative(Network.class), net);
        connect(paxos.getNegative(EventualFailureDetector.class), fd);

        subscribe(decideHandler, rlog);
        subscribe(checkHBHandler, timer);
        subscribe(rangeHandler, store);
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
