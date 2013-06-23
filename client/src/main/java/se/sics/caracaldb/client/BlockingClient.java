/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.utils.TimestampIdFactory;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class BlockingClient {

    // static
    private static final Logger LOG = LoggerFactory.getLogger(BlockingClient.class);
    private static final long TIMEOUT = 1;
    private static final TimeUnit TIMEUNIT = TimeUnit.SECONDS;
    // instance
    private final BlockingQueue<CaracalResponse> responseQueue;
    private final ClientWorker worker;

    BlockingClient(BlockingQueue<CaracalResponse> responseQueue, ClientWorker worker) {
        this.responseQueue = responseQueue;
        this.worker = worker;
    }

    public ResponseCode put(Key k, byte[] value) {
        LOG.debug("Putting on {}", k);
        PutRequest req = new PutRequest(TimestampIdFactory.get().newId(), k, value);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return ResponseCode.CLIENT_TIMEOUT;
            }
            return resp.code;
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return ResponseCode.CLIENT_TIMEOUT;
        }
    }

    public GetResponse get(Key k) {
        LOG.debug("Getting for {}", k);
        long id = TimestampIdFactory.get().newId();
        GetRequest req = new GetRequest(id, k);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new GetResponse(id, k, null, ResponseCode.CLIENT_TIMEOUT);
            }
            if (resp instanceof GetResponse) {
                return (GetResponse) resp;
            }
            return new GetResponse(id, k, null, ResponseCode.UNSUPPORTED_OP);
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new GetResponse(id, k, null, ResponseCode.CLIENT_TIMEOUT);
        }
    }
}
