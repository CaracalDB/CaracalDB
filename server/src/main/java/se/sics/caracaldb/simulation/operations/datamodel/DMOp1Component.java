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
package se.sics.caracaldb.simulation.operations.datamodel;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import se.sics.caracaldb.simulation.operations.OperationsPort;
import java.util.Random;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.datamodel.DMMessage;
import se.sics.caracaldb.datamodel.DMNetworkMessage;
import se.sics.caracaldb.simulation.common.msg.ConnectNode;
import se.sics.caracaldb.simulation.common.msg.TerminateMsg;
import se.sics.caracaldb.simulation.operations.datamodel.cmd.DMTestCmd;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMOp1Component extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DMOp1Component.class);

    Positive<Network> net = requires(Network.class);
    Negative<OperationsPort> opExecutor = provides(OperationsPort.class);

    public Address self = null;
    public Address target = null;
    private final Random randGen;

    
    private final Map<Long, DMMessage.Req> pendingMsg = new HashMap<Long, DMMessage.Req>();
    private Set<DMMessage.Req> bufferedMsg = new HashSet<DMMessage.Req>();

    private boolean terminated = false;

    public DMOp1Component(DMOp1Init init) {
        this.self = init.self;
        this.randGen = new Random(init.randSeed);

        subscribe(terminateHandler, opExecutor);
        subscribe(connectNodeHandler, opExecutor);
        subscribe(testCommandHandler, opExecutor);
    }

    Handler<TerminateMsg.Req> terminateHandler = new Handler<TerminateMsg.Req>() {
        @Override
        public void handle(TerminateMsg.Req event) {
            if (!terminated) {
                terminated = true;
                LOG.info("Received TERMINATE");
                if (bufferedMsg.isEmpty() && pendingMsg.isEmpty()) {
                    LOG.info("Terminating correctly");
                } else {
                    LOG.warn("Still have pending messages, incorrect termination");
                }
            }
        }
    };

    Handler<ConnectNode.Ind> connectNodeHandler = new Handler<ConnectNode.Ind>() {

        @Override
        public void handle(ConnectNode.Ind event) {
            target = event.node;
            tryResendingBuffered();
        }

    };

    Handler<DMTestCmd> testCommandHandler = new Handler<DMTestCmd>() {

        @Override
        public void handle(DMTestCmd event) {
            LOG.debug("test command");
            DMMessage.Req req = new DMMessage.Req(TimestampIdFactory.get().newId());
            if (sendMsg(req)) {
                pendingMsg.put(req.id, req);
            } else {
                bufferedMsg.add(req);
            }
        }
    };

    Handler<DMNetworkMessage.Resp> respHandler = new Handler<DMNetworkMessage.Resp>() {

        @Override
        public void handle(DMNetworkMessage.Resp netResp) {
            LOG.debug("{}: received datamodel resp {} from {}", new Object[]{self, netResp.message, target});
            pendingMsg.remove(netResp.message.id);
            if (netResp.message instanceof DMMessage.Resp) {
                handleTestResp(netResp.message);
            } else {
                LOG.warn("Unknown resp {}. Dropping...", netResp.message);
            }
        }

    };

    private boolean sendMsg(DMMessage.Req req) {
        if (self == null || target == null) {
            LOG.debug("cannot forward msg {}, buffering", req);
            return false;
        }
        LOG.debug("{}: sending datamodel req {} to {}", new Object[]{self, req, target});
        trigger(new DMNetworkMessage.Req(self, target, req), net);
        return true;
    }

    private void tryResendingBuffered() {
        if (bufferedMsg.isEmpty()) {
            return;
        }

        Set<DMMessage.Req> auxBuffer = new HashSet<DMMessage.Req>();
        for (DMMessage.Req req : bufferedMsg) {
            if (sendMsg(req)) {
                pendingMsg.put(req.id, req);
            } else {
                auxBuffer.add(req);
            }
        }

        if (!auxBuffer.isEmpty()) {
            LOG.warn("Sending of messages might be wrong");
        }
        bufferedMsg = auxBuffer;
    }

    private void handleTestResp(DMMessage.Resp resp) {
        LOG.debug("Processing resp {}", resp);
    }
}
