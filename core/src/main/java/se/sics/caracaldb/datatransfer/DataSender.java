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
package se.sics.caracaldb.datatransfer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.flow.ClearToSend;
import se.sics.caracaldb.flow.DataMessage;
import se.sics.caracaldb.flow.RequestToSend;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.RangeReq;
import se.sics.caracaldb.store.RangeResp;
import se.sics.caracaldb.store.TFFactory;
import se.sics.kompics.Handler;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
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
    private final Map<String, Object> metadata;
    private UUID timeoutId;
    private ClearToSend activeCTS;
    private final Queue<ClearToSend> pendingCTS = new LinkedList<>();

    public DataSender(DataSenderInit init) {
        super(init.id);
        range = init.range;
        self = init.self;
        destination = init.destination;
        retryTime = init.retryTime;
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
            SchedulePeriodicTimeout spt
                    = new SchedulePeriodicTimeout(0, retryTime);
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
                ClearToSend cts = new ClearToSend(destination, self, id);
                RequestToSend rts = new RequestToSend();
                rts.setEvent(cts);
                trigger(rts, net);
                trigger(new CancelPeriodicTimeout(timeoutId), timer);
            }
        }
    };
    Handler<ClearToSend> ctsHandler = new Handler<ClearToSend>() {
        @Override
        public void handle(ClearToSend event) {
            if (state != State.WAITING) {
                LOG.info("Got {} while not waiting. Queuing it up.", event);
                pendingCTS.offer(event);
                return;
            }
            activeCTS = event;
            state = State.TRANSFERRING;
            requestData(event.getQuota());
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
                DataMessage finalMsg = activeCTS.constructFinalMessage(new byte[0]);
                trigger(finalMsg, net);
                trigger(new Completed(id), transfer);
                state = State.DONE;
                return;
            }
            lastKey = event.result.lastKey();
            try {
                byte[] data = serialise(event.result);
                DataMessage msg;
                if (data.length < activeCTS.getQuota()) {
                    msg = activeCTS.constructFinalMessage(data);
                    trigger(new Completed(id), transfer);
                    state = State.DONE;
                } else { // It might actually still be final, but we can't really figure that out without trying again
                    msg = activeCTS.constructMessage(data);
                }
                trigger(msg, net);
                dataSent += data.length;
                itemsSent += event.result.size();
            } catch (IOException ex) {
                LOG.error("Could not serialise {}. Ignoring!", event);
            }
            ClearToSend next = pendingCTS.poll();
            if (next != null) {
                activeCTS = next;
                requestData(next.getQuota());
            } else {
                activeCTS = null;
                state = State.WAITING;
            }
        }
    };

    private void requestData(int quota) {
        if (lastKey == null) {
            trigger(new RangeReq(range, Limit.toBytes(quota), TFFactory.tombstoneFilter()), store);
            return;
        }

        KeyRange subRange = KeyRange.open(lastKey).endFrom(range);
        trigger(new RangeReq(subRange, Limit.toBytes(quota), TFFactory.tombstoneFilter()), store);
    }
}
