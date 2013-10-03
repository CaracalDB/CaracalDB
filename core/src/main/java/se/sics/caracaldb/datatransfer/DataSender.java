/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import com.google.common.io.Closer;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.RangeReq;
import se.sics.caracaldb.store.RangeResp;
import se.sics.kompics.Handler;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.DataMessage;
import se.sics.kompics.network.RequestToSend;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DataSender extends DataTransferComponent {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataSender.class);
    
    
    // instance
    
    private final KeyRange range;
    private final Address self;
    private final Address destination;
    private final long retryTime;
    private final int maxSize;
    private final Map<String, Object> metadata;
    private UUID timeoutId;
    private int remainingQuota = 0;
    private TransferClearToSend activeCTS;
    
    
    public DataSender(DataSenderInit init) {
        super(init.id);
        range = init.range;
        self = init.self;
        destination = init.destination;
        retryTime = init.retryTime;
        maxSize = init.maxSize;
        metadata = init.metadata;
        // subscriptions
        subscribe(startHandler, control);
        subscribe(timeoutHandler, timer);
        subscribe(ackHandler, net);
        subscribe(ctsHandler, net);
        subscribe(responseHandler, store);
        subscribe(statusHandler, transfer);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            state = State.INITIALISING;
            SchedulePeriodicTimeout spt =
                    new SchedulePeriodicTimeout(0, retryTime);
            RetryTimeout timeoutEvent = new RetryTimeout(spt);
            spt.setTimeoutEvent(timeoutEvent);
            trigger(spt, timer);
        }
    };
    Handler<RetryTimeout> timeoutHandler = new Handler<RetryTimeout>() {
        @Override
        public void handle(RetryTimeout event) {
            if (state == State.INITIALISING) {
                trigger(new InitiateTransfer(self, destination, id, metadata), net);
            }
        }
    };
    Handler<Ack> ackHandler = new Handler<Ack>() {
        @Override
        public void handle(Ack event) {
            if (state == State.INITIALISING) {
                state = State.WAITING;
                RequestToSend rts = new RequestToSend();
                TransferClearToSend tcts = new TransferClearToSend(destination, self, rts);
                trigger(rts, net);
                trigger(new CancelPeriodicTimeout(timeoutId), timer);
            }
        }
    };
    Handler<TransferClearToSend> ctsHandler = new Handler<TransferClearToSend>() {
        @Override
        public void handle(TransferClearToSend event) {
            if (state != State.WAITING) {
                LOG.warn("Got {} while not waiting. Something is out of place!", event);
                return;
            }
            remainingQuota = event.getQuota();
            activeCTS = event;
            state = State.TRANSFERRING;
            requestData();
        }
    };
    Handler<RangeResp> responseHandler = new Handler<RangeResp>() {
        @Override
        public void handle(RangeResp event) {
            if (state != State.TRANSFERRING) {
                LOG.warn("Got {} while not transferring. Something is out of place!", event);
                return;
            }
            if (event.result.isEmpty()) {
                DataMessage finalMsg = activeCTS.constructMessage(new Complete(self, destination, id));
                finalMsg.setFinal();
                trigger(finalMsg, net);
                trigger(new Completed(id), transfer);
                state = State.DONE;
                return;
            }
            lastKey = event.result.lastKey();
            try {
                byte[] data = serialise(event.result);
                DataMessage msg = activeCTS.constructMessage(new Data(self, destination, id, data));
                trigger(msg, net);
                dataSent += data.length;
                itemsSent += event.result.size();
                remainingQuota--;
                requestData();
            } catch (IOException ex) {
                LOG.error("Could not serialise {}. Ignoring!", event);
            }
        }
    };
    
    private void requestData() {
        if (remainingQuota <= 0) {
            state = State.WAITING;
            return;
        }
        if (lastKey == null) {
            trigger(new RangeReq(range, Limit.toBytes(maxSize), false), store);
            return;
        }
        
        KeyRange subRange = KeyRange.open(lastKey).endFrom(range);
        trigger(new RangeReq(subRange, Limit.toBytes(maxSize), false), store);
    }
}
