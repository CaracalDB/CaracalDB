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

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.util.UUID;
import org.javatuples.Pair;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.flow.ChunkCollector.ClearFlowId;
import se.sics.caracaldb.flow.FlowManager.BufferPointer;
import se.sics.caracaldb.flow.FlowManager.CTS;
import se.sics.caracaldb.flow.FlowManager.Chunk;
import se.sics.caracaldb.flow.FlowManager.RTS;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 *
 * @author lkroll
 */
public class FlowMessageSerializer implements Serializer {

    // Message Type IDs
    private static final Pair<Boolean, Boolean> RTS = Pair.with(false, false);
    private static final Pair<Boolean, Boolean> CTS = Pair.with(false, true);
    private static final Pair<Boolean, Boolean> CHUNK = Pair.with(true, false);
    private static final Pair<Boolean, Boolean> FULL_CHUNK = Pair.with(true, true);

    @Override
    public int identifier() {
        return CoreSerializer.FLOW.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof RTS) {
            rtsToBinary((RTS) o, buf);
            return;
        }
        if (o instanceof CTS) {
            ctsToBinary((CTS) o, buf);
            return;
        }
        if (o instanceof Chunk) {
            chunkToBinary((Chunk) o, buf);
            return;
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Class> hint) {
        MessageFields fields = MessageSerializationUtil.msgFromBinary(buf);
        if ((fields.flag1 == RTS.getValue0()) && (fields.flag2 == RTS.getValue1())) {
            return rtsFromBinary(buf, fields);
        }
        if ((fields.flag1 == CTS.getValue0()) && (fields.flag2 == CTS.getValue1())) {
            return ctsFromBinary(buf, fields);
        }
        if (fields.flag1 == CHUNK.getValue0()) {
            return chunkFromBinary(buf, fields);
        }
        return null;
    }

    // Serializer Pairs
    private void rtsToBinary(RTS rts, ByteBuf buf) {
        MessageSerializationUtil.msgToBinary(rts, buf, RTS.getValue0(), RTS.getValue1());
        SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(rts.flowId, buf);
        buf.writeInt(rts.hint);
    }

    private RTS rtsFromBinary(ByteBuf buf, MessageFields fields) {
        UUID flowId = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        int hint = buf.readInt();
        return new RTS(fields.src, fields.dst, fields.proto, flowId, hint);
    }
    
    private void ctsToBinary(CTS cts, ByteBuf buf) {
        MessageSerializationUtil.msgToBinary(cts, buf, CTS.getValue0(), CTS.getValue1());
        SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(cts.flowId, buf);
        buf.writeInt(cts.clearId);
        buf.writeInt(cts.allowance);
        buf.writeLong(cts.validThrough);
    }
    
    private CTS ctsFromBinary(ByteBuf buf, MessageFields fields) {
        UUID flowId = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        int clearId = buf.readInt();
        int allowance = buf.readInt();
        long valid = buf.readLong();
        return new CTS(fields.src, fields.dst, fields.proto, flowId, clearId, allowance, valid);
    }
    
    private void chunkToBinary(Chunk chunk, ByteBuf buf) {
        boolean full = chunk.data.length == FlowManager.CHUNK_SIZE;
        MessageSerializationUtil.msgToBinary(chunk, buf, CHUNK.getValue0(), full);
        SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(chunk.flowId, buf);
        buf.writeInt(chunk.clearId);
        buf.writeInt(chunk.chunkNo);
        buf.writeInt(chunk.bytes);
        BufferPointer data = chunk.data;
        if (!full) {
            buf.writeInt(data.length);
            buf.writeBoolean(chunk.isFinal);
        }
        buf.writeBytes(data.buffer, data.begin, data.length);
    }
    
    private Chunk chunkFromBinary(ByteBuf buf, MessageFields fields) {
        UUID flowId = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        int clearId = buf.readInt();
        int chunkNo = buf.readInt();
        int bytes = buf.readInt();
        int length = FlowManager.CHUNK_SIZE;
        boolean isFinal = false;
        if (!fields.flag2) {
            length = buf.readInt();
            isFinal = buf.readBoolean();
        }
        ClearFlowId id = new ClearFlowId(flowId, clearId);
        ChunkCollector coll = ChunkCollector.collectors.computeIfAbsent(id, k -> new ChunkCollector(flowId, clearId, bytes));
        BufferPointer data = coll.readChunk(chunkNo, length, buf);
        return new Chunk(fields.src, fields.dst, fields.proto, flowId, clearId, chunkNo, bytes, data, isFinal);
    }
}
