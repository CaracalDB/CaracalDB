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
package se.sics.caracaldb.paxos;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import io.netty.buffer.ByteBuf;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.View;
import se.sics.caracaldb.paxos.Paxos.Accept;
import se.sics.caracaldb.paxos.Paxos.Accepted;
import se.sics.caracaldb.paxos.Paxos.Forward;
import se.sics.caracaldb.paxos.Paxos.Install;
import se.sics.caracaldb.paxos.Paxos.Instance;
import se.sics.caracaldb.paxos.Paxos.NoPromise;
import se.sics.caracaldb.paxos.Paxos.PaxosMsg;
import se.sics.caracaldb.paxos.Paxos.Prepare;
import se.sics.caracaldb.paxos.Paxos.Promise;
import se.sics.caracaldb.paxos.Paxos.Rejected;
import se.sics.caracaldb.replication.log.Reconfigure;
import se.sics.caracaldb.replication.log.Value;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 *
 * @author lkroll
 */
public class PaxosSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(PaxosSerializer.class);

    static final byte PREPARE = 1;
    static final byte PROMISE = 2;
    static final byte NO_PROMISE = 3;
    static final byte ACCEPT = 4;
    static final byte ACCEPTED = 5;
    static final byte REJECTED = 6;
    static final byte INSTALL = 7;
    // Non PaxosMsg
    static final byte FORWARD = 10;

    @Override
    public int identifier() {
        return CoreSerializer.PAXOS.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        // Non PaxosMsg
        if (o instanceof Forward) {
            Forward f = (Forward) o;
            SpecialSerializers.MessageSerializationUtil.msgToBinary(f, buf, true, false);
            buf.writeByte(FORWARD);
            Serializers.toBinary(f.p, buf);
            return;
        }
        if (!(o instanceof PaxosMsg)) {
            return;
        }
        // PaxosMsg
        PaxosMsg msg = (PaxosMsg) o;
        SpecialSerializers.MessageSerializationUtil.msgToBinary(msg, buf, false, false);
        buf.writeInt(msg.ballot);
        if (o instanceof Prepare) {
            buf.writeByte(PREPARE);
            return;
        }
        if (o instanceof Promise) {
            buf.writeByte(PROMISE);
            Promise p = (Promise) o;
            buf.writeInt(p.maxInstances.size());
            for (Instance i : p.maxInstances) {
                instanceToBinary(i, buf);
            }
            CustomSerialisers.serialiseView(p.view, buf);
            return;
        }
        if (o instanceof NoPromise) {
            buf.writeByte(NO_PROMISE);
            return;
        }
        if (o instanceof Accept) {
            buf.writeByte(ACCEPT);
            Accept a = (Accept) o;
            instanceToBinary(a.i, buf);
            return;
        }
        if (o instanceof Accepted) {
            buf.writeByte(ACCEPTED);
            Accepted a = (Accepted) o;
            instanceToBinary(a.i, buf);
            CustomSerialisers.serialiseView(a.view, buf);
            return;
        }
        if (o instanceof Rejected) {
            buf.writeByte(REJECTED);
            Rejected r = (Rejected) o;
            instanceToBinary(r.i, buf);
            return;
        }
        if (o instanceof Install) {
            buf.writeByte(INSTALL);
            Install inst = (Install) o;
            Serializers.toBinary(inst.event, buf);
            buf.writeLong(inst.highestDecided);
            buf.writeInt(inst.log.size());
            for (Entry<Long, Value> e : inst.log.entrySet()) {
                Long k = e.getKey();
                Value v = e.getValue();
                buf.writeLong(k);
                Serializers.toBinary(v, buf);
            }
            return;
        }
        LOG.error("Could not find serializer for {}:{}!", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        MessageFields fields = SpecialSerializers.MessageSerializationUtil.msgFromBinary(buf);
        if (fields.flag1) {
            return otherFromBinary(fields, buf);
        } else {
            return paxosFromBinary(fields, buf);
        }
    }

    private void instanceToBinary(Instance i, ByteBuf buf) {
        buf.writeLong(i.id);
        buf.writeInt(i.ballot);
        Serializers.toBinary(i.value, buf);
    }

    private Instance instanceFromBinary(ByteBuf buf) {
        long id = buf.readLong();
        int ballot = buf.readInt();
        Value v = (Value) Serializers.fromBinary(buf, Optional.absent());
        return new Instance(id, ballot, v);
    }

    private Msg otherFromBinary(MessageFields fields, ByteBuf buf) {
        byte type = buf.readByte();
        Value val;
        switch (type) {
            case FORWARD:
                val = (Value) Serializers.fromBinary(buf, Optional.absent());
                return new Forward(fields.src, fields.dst, fields.orig, val);
            default:
                LOG.error("Unknown Msg type: {}", type);
                return null;
        }
    }

    private PaxosMsg paxosFromBinary(MessageFields fields, ByteBuf buf) {
        int ballot = buf.readInt();
        byte type = buf.readByte();
        View v;
        Instance ins;
        int size;
        switch (type) {
            case PREPARE:
                return new Prepare(fields.src, fields.dst, ballot);
            case PROMISE:
                size = buf.readInt();
                ImmutableSet.Builder<Instance> builder = ImmutableSet.builder();
                for (int i = 0; i < size; i++) {
                    builder.add(instanceFromBinary(buf));
                }
                v = CustomSerialisers.deserialiseView(buf);
                return new Promise(fields.src, fields.dst, ballot, builder.build(), v);
            case NO_PROMISE:
                return new NoPromise(fields.src, fields.dst, ballot);
            case ACCEPT:
                ins = instanceFromBinary(buf);
                return new Accept(fields.src, fields.dst, ballot, ins);
            case ACCEPTED:
                ins = instanceFromBinary(buf);
                v = CustomSerialisers.deserialiseView(buf);
                return new Accepted(fields.src, fields.dst, ballot, ins, v);
            case REJECTED:
                ins = instanceFromBinary(buf);
                return new Rejected(fields.src, fields.dst, ballot, ins);
            case INSTALL:
                Reconfigure rec = (Reconfigure) Serializers.fromBinary(buf, Optional.absent());
                long highestDecided = buf.readLong();
                size = buf.readInt();
                ImmutableSortedMap.Builder<Long, Value> mBuilder = ImmutableSortedMap.naturalOrder();
                for (int i = 0; i < size; i++) {
                    long k = buf.readLong();
                    Value val = (Value) Serializers.fromBinary(buf, Optional.absent());
                    mBuilder.put(k, val);
                }
                return new Install(fields.src, fields.dst, ballot, rec, highestDecided, mBuilder.build());
            default:
                LOG.error("Unknown PaxosMsg type: {}", type);
                return null;
        }
    }

}
