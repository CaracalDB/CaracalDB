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
package se.sics.caracaldb.operations;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.MultiOp;
import se.sics.caracaldb.store.MultiOp.Condition;
import se.sics.caracaldb.store.RangeAction;
import se.sics.caracaldb.store.TransformationFilter;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.BitBuffer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 *
 * @author lkroll
 */
public class OperationSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(OperationSerializer.class);

    private final boolean OP = false;
    private final boolean MSG = true;
    // OPS
    public final boolean REQ = false;
    public final boolean RESP = true;
    // OPS - Types
    public final Boolean[] GET = new Boolean[]{false, false, false};
    public final Boolean[] PUT = new Boolean[]{false, true, false};
    public final Boolean[] RANGE = new Boolean[]{true, false, false};
    public final Boolean[] EMPTY = new Boolean[]{true, true, false};
    public final Boolean[] MULTI = new Boolean[]{false, false, true};
    //public final Boolean[] RANGE2 = new Boolean[] {true, true}; // Never serialised

    @Override
    public int identifier() {
        return CoreSerializer.OP.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof CaracalMsg) {
            int flagPos = buf.writerIndex();
            buf.writeByte(0); // reserve for flags
            BitBuffer flags = BitBuffer.create(MSG); // 0
            toBinaryMsg((CaracalMsg) o, buf, flags);
            byte[] flagsB = flags.finalise();
            buf.setByte(flagPos, flagsB[0]);
            return;
        }
        if (o instanceof CaracalOp) {
            int flagPos = buf.writerIndex();
            buf.writeByte(0); // reserve for flags
            BitBuffer flags = BitBuffer.create(OP); // 0
            toBinaryOp((CaracalOp) o, buf, flags);
            byte[] flagsB = flags.finalise();
            buf.setByte(flagPos, flagsB[0]);
            return;
        }
        LOG.warn("Couldn't serialize {}: {}", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte[] flagsB = new byte[1];
        buf.readBytes(flagsB);
        boolean[] flags = BitBuffer.extract(8, flagsB);
        if (flags[0] == MSG) {
            MessageFields fields = SpecialSerializers.MessageSerializationUtil.msgFromBinary(buf);
            CaracalOp op = fromBinaryOp(buf, flags);
            return new CaracalMsg(fields.src, fields.dst, fields.orig, fields.proto, op);
        }
        if (flags[0] == OP) {
            return fromBinaryOp(buf, flags);
        }
        return null; // no idea how it should get here^^
    }

    private void toBinaryMsg(CaracalMsg caracalMsg, ByteBuf buf, BitBuffer flags) {
        SpecialSerializers.MessageSerializationUtil.msgToBinary(caracalMsg, buf, false, false);
        toBinaryOp(caracalMsg.op, buf, flags);
    }

    private void toBinaryOp(CaracalOp caracalOp, ByteBuf buf, BitBuffer flags) {
        SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(caracalOp.id, buf);
        if (caracalOp instanceof GetRequest) {
            flags.write(REQ); // 1
            flags.write(GET); // 2 3 4
            GetRequest op = (GetRequest) caracalOp;
            CustomSerialisers.serialiseKey(op.key, buf);
            return;
        }
        if (caracalOp instanceof PutRequest) {
            flags.write(REQ); // 1
            flags.write(PUT); // 2 3 4
            PutRequest op = (PutRequest) caracalOp;
            CustomSerialisers.serialiseKey(op.key, buf);
            buf.writeInt(op.data.length);
            buf.writeBytes(op.data);
            return;
        }
        if (caracalOp instanceof RangeQuery.Request) {
            flags.write(REQ); // 1
            flags.write(RANGE); // 2 3 4
            RangeQuery.Request op = (RangeQuery.Request) caracalOp;
            CustomSerialisers.serialiseKeyRange(op.initRange, buf);
            CustomSerialisers.serialiseKeyRange(op.subRange, buf);
            //TODO serialize type once there is more than one
            Serializers.toBinary(op.limitTracker, buf);
            Serializers.toBinary(op.transFilter, buf);
            Serializers.toBinary(op.action, buf);
            return;
        }
        if (caracalOp instanceof MultiOpRequest) {
            flags.write(REQ); // 1
            flags.write(MULTI); // 2 3 4
            MultiOpRequest op = (MultiOpRequest) caracalOp;
            buf.writeInt(op.conditions.size());
            for (Condition c : op.conditions) {
                Serializers.toBinary(c, buf);
            }
            buf.writeInt(op.successPuts.size());
            for (Entry<Key, byte[]> e : op.successPuts.entrySet()) {
                CustomSerialisers.serialiseKey(e.getKey(), buf);
                buf.writeInt(e.getValue().length);
                buf.writeBytes(e.getValue());
            }
            buf.writeInt(op.failurePuts.size());
            for (Entry<Key, byte[]> e : op.failurePuts.entrySet()) {
                CustomSerialisers.serialiseKey(e.getKey(), buf);
                buf.writeInt(e.getValue().length);
                buf.writeBytes(e.getValue());
            }
            return;
        }
        if (caracalOp instanceof GetResponse) {
            flags.write(RESP); // 1
            GetResponse op = (GetResponse) caracalOp;
            buf.writeByte(op.code.id);
            flags.write(GET); // 2 3 4
            CustomSerialisers.serialiseKey(op.key, buf);
            if (op.data == null) {
                flags.write(false); // 5
            } else {
                flags.write(true); // 5
                buf.writeInt(op.data.length);
                buf.writeBytes(op.data);
            }
            return;
        }
        if (caracalOp instanceof PutResponse) {
            flags.write(RESP); // 1
            PutResponse op = (PutResponse) caracalOp;
            buf.writeByte(op.code.id);
            flags.write(PUT); // 2 3 4
            CustomSerialisers.serialiseKey(op.key, buf);
            return;
        }
        if (caracalOp instanceof RangeQuery.Response) {
            flags.write(RESP); // 1
            RangeQuery.Response op = (RangeQuery.Response) caracalOp;
            buf.writeByte(op.code.id);
            flags.write(RANGE); // 2, 3 4
            CustomSerialisers.serialiseKeyRange(op.initRange, buf);
            CustomSerialisers.serialiseKeyRange(op.subRange, buf);
            if (op.data == null) {
                flags.write(false); // 5
            } else {
                flags.write(true); // 5
                buf.writeInt(op.data.size());
                for (Entry<Key, byte[]> e : op.data.entrySet()) {
                    Key k = e.getKey();
                    byte[] v = e.getValue();
                    CustomSerialisers.serialiseKey(k, buf);
                    buf.writeInt(v.length);
                    buf.writeBytes(v);
                }
            }
            flags.write(op.readLimit); // 6
            return;
        }
        if (caracalOp instanceof CaracalResponse) { // Empty Response
            if (!caracalOp.getClass().equals(CaracalResponse.class)) {
                LOG.warn("Should not serialise {} as CaracalResponse!", caracalOp.getClass());
                return;
            }
            flags.write(RESP); // 1
            CaracalResponse op = (CaracalResponse) caracalOp;
            buf.writeByte(op.code.id);
            flags.write(EMPTY); // 2 3 4
            return;
        }
        if (caracalOp instanceof MultiOpResponse) {
            flags.write(RESP); // 1
            MultiOpResponse op = (MultiOpResponse) caracalOp;
            buf.writeByte(op.code.id);
            flags.write(MULTI); // 2 3 4
            flags.write(op.success); // 5
            return;
        }
        LOG.warn("Unkown op type: {}", caracalOp);
    }

    private CaracalOp fromBinaryOp(ByteBuf buf, boolean[] flags) {
        UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        boolean direction = flags[1];
        if (direction == REQ) {
            return fromBinaryReq(buf, flags, id);
        }
        if (direction == RESP) {
            return fromBinaryResp(buf, flags, id);
        }
        return null; // shouldn't get here
    }

    private CaracalOp fromBinaryReq(ByteBuf buf, boolean[] flags, UUID id) {
        if (matches(flags, GET)) {
            Key key = CustomSerialisers.deserialiseKey(buf);
            return new GetRequest(id, key);
        }
        if (matches(flags, PUT)) {
            Key key = CustomSerialisers.deserialiseKey(buf);
            int length = buf.readInt();
            byte[] data = new byte[length];
            buf.readBytes(data);
            return new PutRequest(id, key, data);
        }
        if (matches(flags, RANGE)) {
            KeyRange initRange = CustomSerialisers.deserialiseKeyRange(buf);
            KeyRange subRange = CustomSerialisers.deserialiseKeyRange(buf);
            //TODO deserialize type once there is more than one
            Limit.LimitTracker tracker = (Limit.LimitTracker) Serializers.fromBinary(buf, Optional.absent());
            TransformationFilter filter = (TransformationFilter) Serializers.fromBinary(buf, Optional.absent());
            RangeAction action = (RangeAction) Serializers.fromBinary(buf, Optional.absent());
            return new RangeQuery.Request(id, subRange, initRange, tracker, filter, action, RangeQuery.Type.SEQUENTIAL);
        }
        if (matches(flags, MULTI)) {
            int numC = buf.readInt();
            ImmutableSet.Builder<Condition> cB = ImmutableSet.builder();
            for (int i = 0; i < numC; i++) {
                Condition c = (Condition) Serializers.fromBinary(buf, Optional.absent());
                cB.add(c);
            }
            int numS = buf.readInt();
            ImmutableMap.Builder<Key, byte[]> sPB = ImmutableMap.builder();
            for (int i = 0; i < numS; i++) {
                Key k = CustomSerialisers.deserialiseKey(buf);
                int l = buf.readInt();
                if (l > 0) {
                    byte[] v = new byte[l];
                    buf.readBytes(v);
                    sPB.put(k, v);
                } else {
                    sPB.put(k, null);
                }
            }
            int numF = buf.readInt();
            ImmutableMap.Builder<Key, byte[]> fPB = ImmutableMap.builder();
            for (int i = 0; i < numF; i++) {
                Key k = CustomSerialisers.deserialiseKey(buf);
                int l = buf.readInt();
                if (l > 0) {
                    byte[] v = new byte[l];
                    buf.readBytes(v);
                    fPB.put(k, v);
                } else {
                    fPB.put(k, null);
                }
            }
            return new MultiOpRequest(id, cB.build(), sPB.build(), fPB.build());
        }
        LOG.warn("Got unkown request operation type with flags: {}", flags);
        return null;
    }

    private CaracalOp fromBinaryResp(ByteBuf buf, boolean[] flags, UUID id) {
        ResponseCode code = ResponseCode.byId(buf.readByte());
        if (matches(flags, GET)) {
            Key key = CustomSerialisers.deserialiseKey(buf);
            if (flags[5]) {
                int length = buf.readInt();
                byte[] data = new byte[length];
                buf.readBytes(data);
                return new GetResponse(id, key, data, code);
            } else {
                return new GetResponse(id, key, null, code);
            }
        }
        if (matches(flags, PUT)) {
            Key key = CustomSerialisers.deserialiseKey(buf);
            return new PutResponse(id, key, code);
        }
        if (matches(flags, RANGE)) {
            KeyRange initRange = CustomSerialisers.deserialiseKeyRange(buf);
            KeyRange subRange = CustomSerialisers.deserialiseKeyRange(buf);
            SortedMap<Key, byte[]> result = null;
            if (flags[5]) {
                int size = buf.readInt();
                result = new TreeMap<Key, byte[]>();
                for (int i = 0; i < size; i++) {
                    Key k = CustomSerialisers.deserialiseKey(buf);
                    int length = buf.readInt();
                    if (length > 0) {
                        byte[] data = new byte[length];
                        buf.readBytes(data);
                        result.put(k, data);
                    } else {
                        result.put(k, null);
                    }
                }
            }
            boolean readLimit = flags[6];
            return new RangeQuery.Response(id, code, subRange, initRange, result, readLimit);
        }
        if (matches(flags, EMPTY)) {
            return new CaracalResponse(id, code);
        }
        if (matches(flags, MULTI)) {
            return new MultiOpResponse(id, code, flags[5]);
        }
        LOG.warn("Got unkown response operation type with flags: {}", flags);
        return null;
    }

    private boolean matches(boolean[] flags, Boolean[] type) {
        return (flags[2] == type[0]) && (flags[3] == type[1]) && (flags[4] == type[2]);
    }
}
