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
package se.sics.datamodel.client;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.DMNetworkMessage;
import se.sics.caracaldb.global.SampleRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ClientWorker extends ComponentDefinition {

    private static final Random RAND = new Random();
    private static final Logger LOG = LoggerFactory.getLogger(ClientWorker.class);
    Negative<ClientPort> client = provides(ClientPort.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    // Instance
    private final BlockingQueue<DMMessage.Resp> dataModelQ;
    private final Address self;
    private final Address bootstrapServer;
    private final SortedSet<Address> knownNodes = new TreeSet<Address>();
    private final int sampleSize;
    private Long currentRequestId = -1l;
    private RangeQuery.SeqCollector col;

    public ClientWorker(ClientWorkerInit init) {
        dataModelQ = init.dataModelQ;
        self = init.self;
        bootstrapServer = init.bootstrapServer;
        sampleSize = init.sampleSize;
        knownNodes.add(bootstrapServer);

        // Subscriptions
        subscribe(startHandler, control);
        subscribe(dataModelReqHandler, client);
        subscribe(dataModelRespHandler, net);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.debug("Starting new worker {}", self);
            SampleRequest req = new SampleRequest(self, bootstrapServer, sampleSize);
            trigger(req, net);
        }
    };

    Handler<DMMessage.Req> dataModelReqHandler = new Handler<DMMessage.Req>() {

        @Override
        public void handle(DMMessage.Req event) {
            Address target = randomNode();
            currentRequestId = event.id;
            DMNetworkMessage.Req msg = new DMNetworkMessage.Req(self, target, event);
            LOG.debug("MSG: {}", msg);
            trigger(msg, net);
        }
    };
    Handler<DMNetworkMessage.Resp> dataModelRespHandler = new Handler<DMNetworkMessage.Resp>() {

        @Override
        public void handle(DMNetworkMessage.Resp resp) {
            LOG.debug("Handling Message {}", resp);
            if (resp.message.id != currentRequestId) {
                LOG.debug("Ignoring {} as it has already been received.", resp.message);
                return;
            }
            dmEnqueue(resp.message);
        }

    };

    public void dataModelTrigger(DMMessage.Req req) {
        trigger(req, client.getPair());
    }

    private Address randomNode() {
        int r = RAND.nextInt(knownNodes.size());
        int i = 0;
        for (Address adr : knownNodes) {
            if (r == i) {
                return adr;
            }
            i++;
        }
        return null; // apocalypse oO
    }

    private void dmEnqueue(DMMessage.Resp resp) {
        currentRequestId = -1l;
        if (dataModelQ != null && !dataModelQ.offer(resp)) {
            LOG.warn("Could not insert {} into responseQ. It's overflowing. Clean up this mess!");
        }
    }
}
