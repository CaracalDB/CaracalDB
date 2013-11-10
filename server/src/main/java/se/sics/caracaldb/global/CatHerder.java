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

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.system.StartVNode;
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
    // finals
    private final long heartbeatInterval;
    private final long heartbeatTimeout;
    // instance
    private LookupTable lut;
    private Address self;
    private Set<Address> masterGroup = null;
    private UUID sendHeartbeatId = null;
    private UUID checkHeartbeatsId = null;

    public CatHerder(CatHerderInit init) {
        heartbeatInterval = init.conf.getMilliseconds("caracal.heartbeatInterval");
        heartbeatTimeout = 2 * heartbeatInterval;
        lut = init.bootEvent.lut;
        self = init.self;
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
        subscribe(bootedHandler, maintenance);
        subscribe(forwardMsgHandler, net);
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
            Address[] repGroup = lut.getResponsibles(event.key);
            if (repGroup == null) {
                LOG.warn("No Node found reponsible for key {}! Dropping messsage.", event.key);
                return;
            }
            int nodePos = RAND.nextInt(repGroup.length);
            Address dest = repGroup[nodePos];
            Message msg = event.msg.insertDestination(dest);
            trigger(msg, net);
            LOG.debug("{}: Forwarding {} to {}", new Object[]{self, event.msg, dest});
            //TODO rewrite to check at destination and foward until reached right node
        }
    };
    Handler<ForwardMessage> forwardMsgHandler = new Handler<ForwardMessage>() {
        @Override
        public void handle(ForwardMessage event) {
            Address[] repGroup = lut.getResponsibles(event.forwardTo);
            if (repGroup == null) {
                LOG.warn("No Node found reponsible for key {}! Dropping messsage.", event.forwardTo);
                return;
            }
            int nodePos = RAND.nextInt(repGroup.length);
            Address dest = repGroup[nodePos];
            Message msg = event.msg.insertDestination(dest);
            trigger(msg, net);
            LOG.debug("{}: Forwarding {} to {}", new Object[]{self, event.msg, dest});
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
            //throw new UnsupportedOperationException("Not supported yet.");
        }
        
    };

    private void connectMasterHandlers() {
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
}
