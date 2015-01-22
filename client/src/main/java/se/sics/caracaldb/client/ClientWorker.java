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
package se.sics.caracaldb.client;

import com.google.common.collect.ImmutableSortedSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.global.ForwardMessage;
import se.sics.caracaldb.global.Sample;
import se.sics.caracaldb.global.SampleRequest;
import se.sics.caracaldb.global.Schema;
import se.sics.caracaldb.global.SchemaData;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.MultiOpRequest;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.operations.RangeResponse;
import se.sics.caracaldb.operations.ResponseCode;
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
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ClientWorker extends ComponentDefinition {

    private static final Random RAND = new Random();
    private static final Logger LOG = LoggerFactory.getLogger(ClientWorker.class);
    Negative<ClientPort> client = provides(ClientPort.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    // Instance
    private final BlockingQueue<CaracalResponse> responseQ;
    private final Address self;
    private final Address bootstrapServer;
    private final SortedSet<Address> knownNodes = new TreeSet<Address>();
    private final int sampleSize;
    private UUID currentRequestId = new UUID(-1, -1);
    private RangeQuery.SeqCollector col;
    private final Map<String, CaracalFuture<Schema.Response>> ongoingSchemaRequests
            = new HashMap<String, CaracalFuture<Schema.Response>>();
    private volatile boolean connectionEstablished = false;
    private volatile SchemaData schemas;

    public ClientWorker(ClientWorkerInit init) {
        responseQ = init.q;
        self = init.self;
        bootstrapServer = init.bootstrapServer;
        sampleSize = init.sampleSize;
        knownNodes.add(bootstrapServer);

        // Subscriptions
        subscribe(startHandler, control);
        subscribe(sampleHandler, net);
        subscribe(schemaCreateHandler, client);
        subscribe(schemaDropHandler, client);
        subscribe(multiOpHandler, client);
        subscribe(putHandler, client);
        subscribe(getHandler, client);
        subscribe(rqHandler, client);
        subscribe(responseHandler, net);
        subscribe(schemaResponseHandler, net);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.debug("Starting new worker {}", self);
            SampleRequest req = new SampleRequest(self, bootstrapServer, sampleSize, true);
            trigger(req, net);
        }
    };
    Handler<Sample> sampleHandler = new Handler<Sample>() {
        @Override
        public void handle(Sample event) {
            LOG.debug("Got Sample {}", event);
            knownNodes.addAll(event.nodes);
            schemas = SchemaData.deserialise(event.schemaData);
            connectionEstablished = true;
        }
    };
    Handler<CreateSchema> schemaCreateHandler = new Handler<CreateSchema>() {

        @Override
        public void handle(CreateSchema event) {
            if (!connectionEstablished) {
                event.future.set(new Schema.Response(null, null, event.name, null, false, "No Connection to Cluster!"));
                return;
            }
            byte[] id = schemas.getId(event.name);
            if (id != null) {
                event.future.set(new Schema.Response(null, null, event.name, id, false, "Schema exists!"));
                return;
            }
            Schema.CreateReq req = new Schema.CreateReq(self, bootstrapServer, event.name, event.metaData);
            trigger(req, net);
            ongoingSchemaRequests.put(event.name, event.future);
        }
    };
    Handler<DropSchema> schemaDropHandler = new Handler<DropSchema>() {

        @Override
        public void handle(DropSchema event) {
            if (!connectionEstablished) {
                event.future.set(new Schema.Response(null, null, event.name, null, false, "No Connection to Cluster!"));
                return;
            }
            byte[] id = schemas.getId(event.name);
            if (id == null) {
                event.future.set(new Schema.Response(null, null, event.name, null, false, "Schema does not exists!"));
                return;
            }
            Schema.DropReq req = new Schema.DropReq(self, bootstrapServer, event.name);
            trigger(req, net);
            ongoingSchemaRequests.put(event.name, event.future);
        }
    };
    Handler<MultiOpRequest> multiOpHandler = new Handler<MultiOpRequest>() {

        @Override
        public void handle(MultiOpRequest event) {
            LOG.debug("Handling MultiOp");
            currentRequestId = event.id;
            Address target = randomNode();
            CaracalMsg msg = new CaracalMsg(self, target, event);
            ForwardMessage fmsg = new ForwardMessage(self, target, event.anyKey(), msg);
            trigger(fmsg, net);
            LOG.debug("MSG: {}", fmsg);
        }
    };
    Handler<PutRequest> putHandler = new Handler<PutRequest>() {
        @Override
        public void handle(PutRequest event) {
            LOG.debug("Handling Put {}", event.key);
            currentRequestId = event.id;
            Address target = randomNode();
            CaracalMsg msg = new CaracalMsg(self, target, event);
            ForwardMessage fmsg = new ForwardMessage(self, target, event.key, msg);
            trigger(fmsg, net);
            LOG.debug("MSG: {}", fmsg);
        }
    };
    Handler<GetRequest> getHandler = new Handler<GetRequest>() {
        @Override
        public void handle(GetRequest event) {
            LOG.debug("Handling Get {}", event.key);
            currentRequestId = event.id;
            Address target = randomNode();
            CaracalMsg msg = new CaracalMsg(self, target, event);
            ForwardMessage fmsg = new ForwardMessage(self, target, event.key, msg);
            LOG.debug("MSG: {}", fmsg);
            trigger(fmsg, net);
        }
    };
    Handler<RangeQuery.Request> rqHandler = new Handler<RangeQuery.Request>() {

        @Override
        public void handle(RangeQuery.Request event) {
            LOG.debug("Handling RQ {}", event);
            currentRequestId = event.id;
            Address target = randomNode();
            CaracalMsg msg = new CaracalMsg(self, target, event);
            ForwardMessage fmsg = new ForwardMessage(self, target, event.initRange.begin, msg);
            LOG.debug("MSG: {}", fmsg);
            col = new RangeQuery.SeqCollector(event);
            trigger(fmsg, net);

        }
    };
    Handler<CaracalMsg> responseHandler = new Handler<CaracalMsg>() {
        @Override
        public void handle(CaracalMsg event) {
            knownNodes.add(event.getSource().hostAddress());
            LOG.debug("Handling Message {}", event);
            if (event.op instanceof CaracalResponse) {
                CaracalResponse resp = (CaracalResponse) event.op;
                if (!resp.id.equals(currentRequestId)) {
                    LOG.debug("Ignoring {} as it has already been received.", resp);
                    return;
                }
                if (resp instanceof RangeQuery.Response) {
                    if (col == null) {
                        LOG.debug("Ignoring {} as the request has already been answered.", resp);
                        return;
                    }
                    if (resp.code != ResponseCode.SUCCESS) {
                        enqueue(resp); // abort the query
                        return;
                    }
                    RangeQuery.Response rresp = (RangeQuery.Response) resp;
                    col.processResponse(rresp);
                    if (col.isDone()) {
                        RangeResponse ranger = col.getResponse();
                        col = null;
                        enqueue(ranger);
                    }
                    return;
                }
                enqueue(resp);

                return;
            }
            LOG.error("Sending requests to clients is doing it wrong! {}", event);
        }
    };
    Handler<Schema.Response> schemaResponseHandler = new Handler<Schema.Response>() {

        @Override
        public void handle(Schema.Response event) {
            CaracalFuture<Schema.Response> f = ongoingSchemaRequests.remove(event.name);
            if (f != null) {
                if (event.success) { // request a new sample to update schemas
                    SampleRequest req = new SampleRequest(self, bootstrapServer, sampleSize, true);
                    trigger(req, net);
                }
                f.set(event);
            } else {
                LOG.info("No future stored for schema response {}. Dropping...", event);
            }
        }
    };

    public void triggerOnSelf(CaracalOp op) {
        trigger(op, client.getPair());
    }

    public void triggerOnSelf(SchemaOp op) {
        trigger(op, client.getPair());
    }

    public boolean test() {
        return connectionEstablished;
    }

    public Key resolveSchema(String schema, Key k) {
        if (connectionEstablished) {
            byte[] schemaId = schemas.getId(schema);
            if (schemaId != null) {
                return k.prepend(schemaId).get();
            } else { // try to update the schema data (might want to rate-limit this a bit)
                SampleRequest req = new SampleRequest(self, bootstrapServer, sampleSize, true);
                trigger(req, net);
            }
        }
        return null;
    }

    public KeyRange resolveSchema(String schema, KeyRange k) {
        if (connectionEstablished) {
            byte[] schemaId = schemas.getId(schema);
            if (schemaId != null) {
                Key startK = k.begin.prepend(schemaId).get();
                Key endK = k.end.prepend(schemaId).get();
                return k.replaceKeys(startK, endK);
            } else { // try to update the schema data (might want to rate-limit this a bit)
                SampleRequest req = new SampleRequest(self, bootstrapServer, sampleSize, true);
                trigger(req, net);
            }
        }
        return null;
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

    private void enqueue(CaracalResponse resp) {
        currentRequestId = new UUID(-1, -1);
        trigger(resp, client);
        if (responseQ != null && !responseQ.offer(resp)) {
            LOG.warn("Could not insert {} into responseQ. It's overflowing. Clean up this mess!");
        }
    }

    public ImmutableSortedSet<String> listSchemas() {
        if (connectionEstablished) {
            return schemas.schemas();
        }
        return ImmutableSortedSet.of();
    }

    String getSchemaInfo(String schemaName) {
        if (connectionEstablished) {
            return schemas.schemaInfo(schemaName);
        }
        return "No connection!";
    }

}
