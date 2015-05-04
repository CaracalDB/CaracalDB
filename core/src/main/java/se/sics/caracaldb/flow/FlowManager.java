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
package se.sics.caracaldb.flow;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.math.LongMath;
import com.larskroll.common.DataRef;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.SortedSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.BaseMessage;
import se.sics.caracaldb.CoreSerializer;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.network.MessageNotify;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author lkroll
 */
public class FlowManager extends ComponentDefinition {

    static final Logger LOG = LoggerFactory.getLogger(FlowManager.class);
    static final long LEASE_TIME = 10 * 60 * 1000; // 10min
    static final long CLEANUP_PERIOD = 60 * 1000; // 1min
    public static final int CHUNK_SIZE = 65000;

    static {
        Serializers.register(CoreSerializer.FLOW.instance, "flowS");
        Serializers.register(FlowMessage.class, "flowS");
    }

    // Ports
    final Negative<DataFlow> flow = provides(DataFlow.class);
    final Positive<Network> net = requires(Network.class);
    final Positive<Timer> timer = requires(Timer.class);
    // Instance
    private Address self;
    private Transport protocol;
    private long minAlloc;
    private long bufferSize;
    private long freeBuffer;
    private final Map<UUID, ClearToSend> requestedFlows = new HashMap<UUID, ClearToSend>();
    private final Queue<RTS> openRequests = new LinkedList<RTS>();
    private final Queue<CTS> openClears = new LinkedList<CTS>();
    private final Map<UUID, Integer> clearIds = new HashMap<UUID, Integer>();
    private final ArrayListMultimap<UUID, CTS> promises = ArrayListMultimap.create();
    private final Map<UUID, Chunk> pendingChunks = new HashMap<UUID, Chunk>();
    private UUID timeoutId = null;

    public FlowManager(FlowManagerInit init) {
        // Init
        minAlloc = init.minAlloc;
        bufferSize = init.bufferSize;
        freeBuffer = bufferSize;
        protocol = init.protocol;
        self = init.self;
        // subscriptions
        subscribe(startHandler, control);
        subscribe(stopHandler, control);
        subscribe(freeHandler, timer);
        subscribe(rtsHandler, flow);
        subscribe(dataHandler, flow);
        subscribe(rts2Handler, net);
        subscribe(ctsHandler, net);
        subscribe(notifyRespHandler, net);
        subscribe(chunkHandler, net);
    }

    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(CLEANUP_PERIOD, CLEANUP_PERIOD);
            FreeTime ft = new FreeTime(spt);
            spt.setTimeoutEvent(ft);
            timeoutId = ft.getTimeoutId();
            trigger(spt, timer);
        }
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            if (timeoutId != null) {
                CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(timeoutId);
            }
        }
    };
    Handler<FreeTime> freeHandler = new Handler<FreeTime>() {

        @Override
        public void handle(FreeTime event) {
            long time = System.currentTimeMillis();
            for (Iterator<CTS> it = promises.values().iterator(); it.hasNext();) {
                CTS v = it.next();
                if (v.validThrough > time) {
                    freeBuffer += v.allowance;
                    it.remove();
                }
            }
            tryAssigningClears();
            tryAssigningRequests();
        }
    };
    Handler<RequestToSend> rtsHandler = new Handler<RequestToSend>() {

        @Override
        public void handle(RequestToSend e) {
            LOG.info("Asking for space for: {}", e);
            ClearToSend cts = e.getEvent();
            requestedFlows.put(cts.getFlowId(), cts);
            Address src = (cts.getSource() == null) ? self : cts.getSource();
            RTS rts = new RTS(src, cts.getDestination(), protocol, cts.getFlowId(), e.hint);
            trigger(rts, net);
        }
    };
    Handler<DataMessage> dataHandler = new Handler<DataMessage>() {

        @Override
        public void handle(DataMessage e) {
            LOG.info("Got data to chunk: {}", e);
            long numberOfChunks = Chunk.numberOfChunks(e.data.size());
            Address dst = requestedFlows.get(e.flowId).getDestination();
            long i = 0;
            Iterator<DataRef> it = e.data.split(numberOfChunks, CHUNK_SIZE).iterator();
            while (it.hasNext()) {
                DataRef part = it.next();
                Chunk c = new Chunk(self, dst, protocol, e.flowId, e.clearId, i, e.data.size(), part, !it.hasNext() && e.isfinal, e.collector);
                MessageNotify.Req req = MessageNotify.create(c);
                pendingChunks.put(req.getMsgId(), c);
                trigger(req, net);
                i++;
            }
            LOG.debug("Got {} outstanding chunks to be sent.", pendingChunks.size());
            if (!e.isfinal) { // Request new space immediately
                ClearToSend ctsE = requestedFlows.get(e.flowId);
                Address src = (ctsE.getSource() == null) ? self : ctsE.getSource();
                RTS rts = new RTS(src, ctsE.getDestination(), protocol, ctsE.getFlowId(), e.data.size());
                trigger(rts, net);
            }
        }
    };
    Handler<RTS> rts2Handler = new Handler<RTS>() {

        @Override
        public void handle(RTS e) {
            if (!clearIds.containsKey(e.flowId)) {
                clearIds.put(e.flowId, 0);
            }
            openRequests.offer(e); // Assign requests in FIFO order
            LOG.debug("Got {}. Trying to assign space...");
            tryAssigningRequests();
        }
    };
    Handler<CTS> ctsHandler = new Handler<CTS>() {

        @Override
        public void handle(CTS e) {
            if (e.validThrough < System.currentTimeMillis()) {
                LOG.error("You have badly synchronized clocks in your system. "
                        + "Some data flows might not make any progress. "
                        + "Offending nodes: {} -> {}",
                        e.getSource(), e.getDestination());
            }
            openClears.offer(e); // Assign clears in FIFO order
            LOG.debug("Got {}. Trying to assign space...");
            tryAssigningClears();
        }
    };
    Handler<MessageNotify.Resp> notifyRespHandler = new Handler<MessageNotify.Resp>() {

        @Override
        public void handle(MessageNotify.Resp e) {
            Chunk c = pendingChunks.remove(e.msgId);
            freeBuffer += c.data.size();
            LOG.debug("Got {} outstanding chunks to be sent and {}bytes free space", pendingChunks.size(), freeBuffer);
            c.data.release();
            tryAssigningClears();
            if (c.isLast()) { // After the last piece assign remote requests if there is buffer left
                tryAssigningRequests();
            }
        }
    };
    Handler<Chunk> chunkHandler = new Handler<Chunk>() {

        @Override
        public void handle(Chunk e) {
            LOG.debug("Got chunk {}" + e);
            if (e.isLast()) { // Only act on getting the last chunk for a datamessage
                ChunkCollector coll = ChunkCollector.collectors.remove(ChunkCollector.getIdFor(e.flowId, e.clearId));
                if (coll == null) {
                    LOG.error("Didn't get a ChunkCollector for a deserialized message!");
                    return;
                }
                if (!coll.isComplete()) {
                    LOG.error("Got the last message on a clearId but Collector does not report complete!");
                    SortedSet<Long> missing = coll.getMissingChunks();
                    LOG.debug("Missing chunks:");
                    for (Long l : missing) {
                        LOG.debug("Chunk# {}", l);
                    }
                    return;
                }
                DataMessage dm = new DataMessage(e.flowId, e.clearId, coll.getResult(), e.collector, e.isFinal);
                trigger(dm, flow);
            }
            // TODO consider if handling lost messages is worth the effort}
        }
    };

    private void assignSpace(RTS rts) { // remote assign
        long allowance;
        if (rts.hint > 0) { // Need finite amount of space
            if (rts.hint <= freeBuffer) { // amount fits into free space
                allowance = Math.max(rts.hint, minAlloc);
            } else { // need more space than available
                allowance = freeBuffer; //TODO maybe come up with a better strategy
            }
        } else { // Need unkown amount of space
            allowance = freeBuffer; //TODO maybe come up with a better strategy
        }
        freeBuffer -= allowance;
        long validThrough = System.currentTimeMillis() + LEASE_TIME;
        Integer clearId = clearIds.get(rts.flowId);
        if (clearId != null) {
            clearId++;
            clearIds.put(rts.flowId, clearId);
        }
        CTS cts = new CTS(self, rts.getSource(), protocol, rts.flowId, clearId, allowance, validThrough);
        promises.put(rts.flowId, cts);
        trigger(cts, net);
        LOG.debug("Assigned {}bytes (remaining: {}bytes) to {}", allowance, freeBuffer, cts);
    }

    private void assignSpace(CTS cts) {
        try {
            // local assign
            long allowance = Math.min(freeBuffer, cts.allowance);
            freeBuffer -= allowance;
            ClearToSend clear = (ClearToSend) requestedFlows.get(cts.flowId).clone();
            clear.setClearId(cts.clearId);
            clear.setQuota(allowance);
            trigger(clear, flow);
            LOG.debug("Assigned {}bytes to {}", allowance, cts);
        } catch (CloneNotSupportedException ex) {
            LOG.warn("Clould not clone CTS!", ex);
        }
    }

    private void tryAssigningRequests() {
        while ((freeBuffer >= minAlloc) && !openRequests.isEmpty()) {
            assignSpace(openRequests.poll());
        }
    }

    private void tryAssigningClears() {
        long time = System.currentTimeMillis();
        while ((freeBuffer >= minAlloc) && !openClears.isEmpty()) {
            CTS cts = openClears.poll();
            if (cts.validThrough > time) {
                LOG.debug("Assigning space to {}", cts);
                assignSpace(cts);
            } else { // Request a more up to date CTS
                LOG.debug("{} has been invalidated. Requesting new one.", cts);
                ClearToSend ctsE = requestedFlows.get(cts.flowId);
                Address src = (ctsE.getSource() == null) ? self : ctsE.getSource();
                RTS rts = new RTS(src, ctsE.getDestination(), protocol, ctsE.getFlowId(), cts.allowance);
                trigger(rts, net);
            }
        }
    }

    public static abstract class FlowMessage extends BaseMessage {

        public final UUID flowId;

        public FlowMessage(Address src, Address dst, Transport protocol, UUID flowId) {
            super(src, dst, protocol);
            this.flowId = flowId;
        }

        protected void headerToString(StringBuilder sb) {
            sb.append("Header(src: ");
            sb.append(this.getSource());
            sb.append(", dst: ");
            sb.append(this.getDestination());
            sb.append(", protocol: ");
            sb.append(this.getProtocol().name());
            sb.append(") on flow ");
            sb.append(flowId);
        }
        
    }

    public static class RTS extends FlowMessage {

        public final long hint;

        public RTS(Address src, Address dst, Transport protocol, UUID flowId, long hint) {
            super(src, dst, protocol, flowId);
            this.hint = hint;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("RTS(\n");
            headerToString(sb);
            sb.append("\n hint: ");
            sb.append(hint);
            sb.append(")");
            return sb.toString();
        }
    }

    public static class CTS extends FlowMessage {

        public final int clearId;
        public final long allowance;
        public final long validThrough;

        public CTS(Address src, Address dst, Transport protocol, UUID flowId, int clearId, long allowance, long validThrough) {
            super(src, dst, protocol, flowId);
            this.clearId = clearId;
            this.allowance = allowance;
            this.validThrough = validThrough;
        }
    
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("CTS(\n");
            headerToString(sb);
            sb.append("\n clearId: ");
            sb.append(clearId);
            sb.append(", allowance: ");
            sb.append(allowance);
            sb.append("bytes, validThrough: ");
            sb.append(validThrough);
            sb.append(")");
            return sb.toString();
        }
    }

    public static class Chunk extends FlowMessage {

        public final int clearId;
        public final long chunkNo; // number of this chunk
        public final long bytes; // overall bytes on this clearId
        public final DataRef data;
        public final boolean isFinal;
        public final CollectorDescriptor collector;

        public Chunk(Address src, Address dst, Transport protocol, UUID flowId, int clearId, long chunkNo, long bytes, DataRef data, boolean isFinal, CollectorDescriptor collector) {
            super(src, dst, protocol, flowId);
            this.clearId = clearId;
            this.chunkNo = chunkNo;
            this.bytes = bytes;
            this.data = data;
            this.isFinal = isFinal;
            this.collector = collector;
        }

        public long numberOfChunks() {
            return Chunk.numberOfChunks(bytes);
        }

        public static long numberOfChunks(long bytes) {
            return LongMath.divide(bytes, FlowManager.CHUNK_SIZE, RoundingMode.UP);
        }

        public boolean isLast() {
            return chunkNo == (numberOfChunks() - 1);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Chunk(\n");
            headerToString(sb);
            sb.append("\n clearId: ");
            sb.append(clearId);
            sb.append(", chunk#: ");
            sb.append(chunkNo);
            sb.append(", bytes: ");
            sb.append(bytes);
            if (isFinal) {
                sb.append(", final");
            } else {
                sb.append(", not final");
            }
            sb.append(", collector: \n");
            sb.append(collector);
            sb.append("\n)");
            return sb.toString();
        }
        
    }

    public static class FreeTime extends Timeout {

        public FreeTime(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }

}
