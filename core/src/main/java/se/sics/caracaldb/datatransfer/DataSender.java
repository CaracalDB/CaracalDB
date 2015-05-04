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

import com.google.common.collect.ImmutableMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.flow.ClearToSend;
import se.sics.caracaldb.flow.DataMessage;
import se.sics.caracaldb.flow.RequestToSend;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DataSender extends DataTransferComponent {

    private static final Logger LOG = LoggerFactory.getLogger(DataSender.class);

    // Ports
    Positive<DataSource> source = requires(DataSource.class);
    // instance   
    private final Address self;
    private final Address destination;
    private final long retryTime;
    private final ImmutableMap<String, Object> metadata;
    private UUID timeoutId;
    private ClearToSend activeCTS;
    private final Queue<ClearToSend> pendingCTS = new LinkedList<ClearToSend>();
    private long minQuota = -1;

    public DataSender(DataSenderInit init) {
        super(init.id);
        self = init.self;
        destination = init.destination;
        retryTime = init.retryTime;
        metadata = ImmutableMap.copyOf(init.metadata);
        // subscriptions
        subscribe(startHandler, control);
        subscribe(timeoutHandler, timer);
        subscribe(ackHandler, net);
        subscribe(ctsHandler, flow);
        subscribe(dataHandler, source);
        subscribe(requirementsHandler, source);
        subscribe(statusHandler, transfer);
        subscribe(receivedHandler, net);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            state = State.INITIALISING;
            SchedulePeriodicTimeout spt
                    = new SchedulePeriodicTimeout(0, retryTime);
            RetryTimeout timeoutEvent = new RetryTimeout(spt);
            spt.setTimeoutEvent(timeoutEvent);
            timeoutId = timeoutEvent.getTimeoutId();
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
                ClearToSend cts = new ClearToSend(self, destination, id);
                RequestToSend rts = new RequestToSend(minQuota);
                rts.setEvent(cts);
                trigger(rts, flow);
                trigger(new CancelPeriodicTimeout(timeoutId), timer);
                LOG.info("Got Ack. WAITING for CTS.");
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
            LOG.debug("Requesting data: {}", event);
            trigger(new Data.Request(event.getQuota()), source);
        }
    };
    Handler<Data.Requirements> requirementsHandler = new Handler<Data.Requirements>() {

        @Override
        public void handle(Data.Requirements event) {
            LOG.info("Transfer requires {}bytes of allocated space", event.minQuota);
            minQuota = event.minQuota;
        }
    };
    Handler<AllReceived> receivedHandler = new Handler<AllReceived>() {

        @Override
        public void handle(AllReceived event) {
            trigger(new Completed(id), transfer);
        }
    };
    Handler<Data.Reference> dataHandler = new Handler<Data.Reference>() {

        @Override
        public void handle(Data.Reference event) {
            if (state != DataTransferComponent.State.TRANSFERRING) {
                LOG.warn("Got {} while not transferring. Something is out of place!", event);
                return;
            }
            DataMessage msg;
            if (event.isFinal) {
                msg = activeCTS.constructFinalMessage(event.data, event.collector);
                state = DataTransferComponent.State.DONE;
            } else {
                msg = activeCTS.constructMessage(event.data, event.collector);
            }
            trigger(msg, flow);
            LOG.debug("Sending data to flow: {}", msg);
            dataSent += event.data.size();
            ClearToSend next = pendingCTS.poll();
            if (next != null) {
                activeCTS = next;
                trigger(new Data.Request(next.getQuota()), source);
            } else {
                activeCTS = null;
                state = DataTransferComponent.State.WAITING;
            }
        }
    };

}
