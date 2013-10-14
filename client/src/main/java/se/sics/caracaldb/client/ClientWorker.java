/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.client;

import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.global.ForwardMessage;
import se.sics.caracaldb.global.Sample;
import se.sics.caracaldb.global.SampleRequest;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.PutRequest;
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
    private Long currentRequestId;

    public ClientWorker(ClientWorkerInit init) {
        responseQ = init.q;
        self = init.self;
        bootstrapServer = init.bootstrapServer;
        sampleSize = init.sampleSize;
        knownNodes.add(bootstrapServer);

        // Subscriptions
        subscribe(startHandler, control);
        subscribe(sampleHandler, net);
        subscribe(putHandler, client);
        subscribe(getHandler, client);
        subscribe(responseHandler, net);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            LOG.debug("Starting new worker {}", self);
            SampleRequest req = new SampleRequest(self, bootstrapServer, sampleSize);
            trigger(req, net);
        }
    };
    Handler<Sample> sampleHandler = new Handler<Sample>() {
        @Override
        public void handle(Sample event) {
            LOG.debug("Got Sample {}", event);
            knownNodes.addAll(event.nodes);
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
    Handler<CaracalMsg> responseHandler = new Handler<CaracalMsg>() {
        @Override
        public void handle(CaracalMsg event) {
            LOG.debug("Handling Message {}", event);
            if (event.op instanceof CaracalResponse) {
                CaracalResponse resp = (CaracalResponse) event.op;
                if (resp.id != currentRequestId) {
                    LOG.debug("Ignoring {} as it has already been received.", resp);
                    return;
                }
                trigger(resp, client);
                if (responseQ != null && !responseQ.offer(resp)) {
                    LOG.warn("Could not insert {} into responseQ. It's overflowing. Clean up this mess!");
                }
                return;
            }
            LOG.error("Sending requests to client is doing it wrong! {}", event);
        }
    };

    public void triggerOnSelf(CaracalOp op) {
        trigger(op, client.getPair());
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
}