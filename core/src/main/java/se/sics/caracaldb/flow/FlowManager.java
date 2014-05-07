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
import com.google.common.math.IntMath;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.CoreSerializer;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.MessageNotify;
import se.sics.kompics.network.Msg;
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
    static final int CHUNK_SIZE = 65000;
    
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
    private int minAlloc;
    private int bufferSize;
    private int freeBuffer;
    private final Map<UUID, ClearToSend> requestedFlows = new HashMap<>();
    private final Queue<RTS> openRequests = new LinkedList<>();
    private final Queue<CTS> openClears = new LinkedList<>();
    private final Map<UUID, Integer> clearIds = new HashMap<>();
    private final ArrayListMultimap<UUID, CTS> promises = ArrayListMultimap.create();
    private final Map<UUID, Chunk> pendingChunks = new HashMap<>();
    private UUID timeoutId = null;
    
    public FlowManager(FlowManagerInit init) {
        // Init
        minAlloc = init.minAlloc;
        bufferSize = init.bufferSize;
        freeBuffer = bufferSize;
        protocol = init.protocol;
        self = init.self;
    }
    
    {
        handle(control, Start.class, e -> {
            SchedulePeriodicTimeout spt = new SchedulePeriodicTimeout(CLEANUP_PERIOD, CLEANUP_PERIOD);
            FreeTime ft = new FreeTime(spt);
            spt.setTimeoutEvent(ft);
            timeoutId = ft.getTimeoutId();
            trigger(spt, timer);
        });
        
        handle(control, Stop.class, e -> {
            if (timeoutId != null) {
                CancelPeriodicTimeout cpt = new CancelPeriodicTimeout(timeoutId);
            }
        });
        
        handle(timer, FreeTime.class, e -> {
            long time = System.currentTimeMillis();
            promises.values().removeIf(v -> {
                if (v.validThrough > time) {
                    freeBuffer += v.allowance;
                    return true;
                }
                return false;
            });
            tryAssigningClears();
            tryAssigningRequests();
        });
        
        handle(flow, RequestToSend.class, e -> {
            ClearToSend cts = e.getEvent();
            requestedFlows.put(cts.getFlowId(), cts);
            Address src = (cts.getSource() == null) ? self : cts.getSource();
            RTS rts = new RTS(src, cts.getDestination(), protocol, cts.getFlowId(), e.hint);
            trigger(rts, net);
        });
        
        handle(flow, DataMessage.class, e -> {
            byte[] data = e.data;
            int numberOfChunks = Chunk.numberOfChunks(data.length);
            int fullChunks = numberOfChunks - 1;
            Address dst = requestedFlows.get(e.flowId).getDestination();
            for (int i = 0; i < fullChunks; i++) {
                BufferPointer p = new BufferPointer(i * CHUNK_SIZE, CHUNK_SIZE, data);
                Chunk c = new Chunk(self, dst, protocol, e.flowId, e.clearId, i, data.length, p, false);
                MessageNotify.Req req = MessageNotify.create(c);
                pendingChunks.put(req.getMsgId(), c);
                trigger(req, net);
            }
            int remainder = data.length - fullChunks * CHUNK_SIZE;
            BufferPointer p = new BufferPointer(fullChunks * CHUNK_SIZE, remainder, data);
            Chunk c = new Chunk(self, dst, protocol, e.flowId, e.clearId, fullChunks, data.length, p, e.isfinal);
            MessageNotify.Req req = MessageNotify.create(c);
            pendingChunks.put(req.getMsgId(), c);
            trigger(req, net);
            if (!e.isfinal) { // Request new space immediately
                ClearToSend ctsE = requestedFlows.get(e.flowId);
                Address src = (ctsE.getSource() == null) ? self : ctsE.getSource();
                RTS rts = new RTS(src, ctsE.getDestination(), protocol, ctsE.getFlowId(), data.length);
                trigger(rts, net);
            }
        });
        
        handle(net, RTS.class, e -> {
            if (!clearIds.containsKey(e.flowId)) {
                clearIds.put(e.flowId, 0);
            }
            openRequests.offer(e); // Assign requests in FIFO order
            tryAssigningRequests();
        });
        
        handle(net, CTS.class, e -> {
            if (e.validThrough < System.currentTimeMillis()) {
                LOG.error("You have badly synchronized clocks in your system. "
                        + "Some data flows might not make any progress. "
                        + "Offending nodes: {} -> {}",
                        e.getSource(), e.getDestination());
            }
            openClears.offer(e); // Assign clears in FIFO order
            tryAssigningClears();
        });
        
        handle(net, MessageNotify.Resp.class, e -> {
            Chunk c = pendingChunks.remove(e.msgId);
            freeBuffer += c.data.length;
            tryAssigningClears();
            if (c.isLast()) { // After the last piece assign remote requests if there is buffer left
                tryAssigningRequests();
            }
        });
        
        handle(net, Chunk.class, e -> {
            LOG.debug("Got chunk {}" + e);
            if (e.isLast()) { // Only act on getting the last chunk for a datamessage
                ChunkCollector coll = ChunkCollector.collectors.remove(ChunkCollector.getIdFor(e.flowId, e.clearId));
                if (coll == null) {
                    LOG.error("Didn't get a ChunkCollector for a deserialized message!");
                    return;
                }
                if (!coll.isComplete()) {
                    LOG.error("Got the last message on a clearId but Collector does not report complete!");
                    return;
                }
                DataMessage dm = new DataMessage(e.flowId, e.clearId, coll.data, e.isFinal);
                trigger(dm, flow);
            }
            // TODO consider if handling lost messages is worth the effort
        });
        
    }
    
    private void assignSpace(RTS rts) { // remote assign
        int allowance;
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
        int clearId = clearIds.computeIfPresent(rts.flowId, (k, v) -> v + 1);
        CTS cts = new CTS(self, rts.getSource(), protocol, rts.flowId, clearId, allowance, validThrough);
        promises.put(rts.flowId, cts);
        trigger(cts, net);
    }
    
    private void assignSpace(CTS cts) {
        try {
            // local assign
            int allowance = Math.min(freeBuffer, cts.allowance);
            freeBuffer -= allowance;
            ClearToSend clear = (ClearToSend) requestedFlows.get(cts.flowId).clone();
            clear.setClearId(cts.clearId);
            clear.setQuota(allowance);
            trigger(clear, flow);
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
                assignSpace(cts);
            } else { // Request a more up to date CTS
                ClearToSend ctsE = requestedFlows.get(cts.flowId);
                Address src = (ctsE.getSource() == null) ? self : ctsE.getSource();
                RTS rts = new RTS(src, ctsE.getDestination(), protocol, ctsE.getFlowId(), cts.allowance);
                trigger(rts, net);
            }
        }
    }
    
    public static abstract class FlowMessage implements Msg {
        
        public final Address source;
        public final Address destination;
        public final Transport protocol;
        
        public final UUID flowId;
        
        public FlowMessage(Address src, Address dst, Transport protocol, UUID flowId) {
            this.source = src;
            this.destination = dst;
            this.protocol = protocol;
            this.flowId = flowId;
        }
        
        @Override
        public Address getSource() {
            return this.source;
        }
        
        @Override
        public Address getDestination() {
            return this.destination;
        }
        
        @Override
        public Address getOrigin() {
            return this.source;
        }
        
        @Override
        public Transport getProtocol() {
            return this.protocol;
        }
    }
    
    public static class RTS extends FlowMessage {
        
        public final int hint;
        
        public RTS(Address src, Address dst, Transport protocol, UUID flowId, int hint) {
            super(src, dst, protocol, flowId);
            this.hint = hint;
        }
    }
    
    public static class CTS extends FlowMessage {
        
        public final int clearId;
        public final int allowance;
        public final long validThrough;
        
        public CTS(Address src, Address dst, Transport protocol, UUID flowId, int clearId, int allowance, long validThrough) {
            super(src, dst, protocol, flowId);
            this.clearId = clearId;
            this.allowance = allowance;
            this.validThrough = validThrough;
        }
    }
    
    public static class Chunk extends FlowMessage {
        
        public final int clearId;
        public final int chunkNo; // number of this chunk
        public final int bytes; // overall bytes on this clearId
        public final BufferPointer data;
        public final boolean isFinal;
        
        public Chunk(Address src, Address dst, Transport protocol, UUID flowId, int clearId, int chunkNo, int bytes, BufferPointer data, boolean isFinal) {
            super(src, dst, protocol, flowId);
            this.clearId = clearId;
            this.chunkNo = chunkNo;
            this.bytes = bytes;
            this.data = data;
            this.isFinal = isFinal;
        }
        
        public int numberOfChunks() {
            return Chunk.numberOfChunks(bytes);
        }
        
        public static int numberOfChunks(int bytes) {
            return IntMath.divide(bytes, FlowManager.CHUNK_SIZE, RoundingMode.UP);
        }
        
        public boolean isLast() {
            return chunkNo == (numberOfChunks() - 1);
        }
        
    }
    
    public static class FreeTime extends Timeout {
        
        public FreeTime(SchedulePeriodicTimeout spt) {
            super(spt);
        }
    }
    
    public static class BufferPointer {
        
        public final int begin;
        public final int length;
        public final byte[] buffer;
        
        public BufferPointer(int begin, int length, byte[] buffer) {
            this.begin = begin;
            this.length = length;
            this.buffer = buffer;
        }
    }
}
