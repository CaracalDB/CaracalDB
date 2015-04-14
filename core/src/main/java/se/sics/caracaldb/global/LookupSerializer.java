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
package se.sics.caracaldb.global;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.larskroll.common.BitBuffer;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.AddressSerializer;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.MessageSerializationUtil;
import se.sics.caracaldb.MessageSerializationUtil.MessageFields;
import se.sics.caracaldb.global.SchemaData.SingleSchema;
import static se.sics.caracaldb.global.SchemaData.deserialiseSchema;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

/**
 *
 * @author lkroll
 */
public class LookupSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(LookupSerializer.class);

    private static final Boolean[] FORWARD = new Boolean[]{false, false};
    private static final Boolean[] SCHEMA = new Boolean[]{false, true};
    private static final Boolean[] SAMPLE = new Boolean[]{true, false};
    private static final Boolean[] LUT = new Boolean[]{true, true};
    // SCHEMA-
    private static final Boolean[] CREATE = new Boolean[]{false, false};
    private static final Boolean[] DROP = new Boolean[]{false, true};
    private static final Boolean[] RESP = new Boolean[]{true, false};
    // SAMPLE-
    private static final Boolean[] SREQ = new Boolean[]{false, false};
    private static final Boolean[] SRESP = new Boolean[]{false, true};
    // LUT-
    private static final Boolean[] LOUTDATED = new Boolean[]{false, false};
    private static final Boolean[] LPART = new Boolean[]{false, true};

    @Override
    public int identifier() {
        return CoreSerializer.LOOKUP.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof ForwardMessage) {
            ForwardMessage msg = (ForwardMessage) o;
            MessageSerializationUtil.msgToBinary(msg, buf, FORWARD[0], FORWARD[1]);
            CustomSerialisers.serialiseKey(msg.forwardTo, buf);
            Serializers.toBinary(msg.msg, buf);
            return;
        }
        if (o instanceof SampleRequest) {
            SampleRequest msg = (SampleRequest) o;
            MessageSerializationUtil.msgToBinary(msg, buf, SAMPLE[0], SAMPLE[1]);
            BitBuffer flags = BitBuffer.create(SREQ); // 0 and 1
            flags.write(msg.schemas); // 2
            flags.write(msg.lut); // 3
            byte[] flagsB = flags.finalise();
            buf.writeBytes(flagsB);
            buf.writeInt(msg.n);
            buf.writeLong(msg.lutversion);
            return;
        }
        if (o instanceof Sample) {
            Sample msg = (Sample) o;
            MessageSerializationUtil.msgToBinary(msg, buf, SAMPLE[0], SAMPLE[1]);
            BitBuffer flags = BitBuffer.create(SRESP);
            byte[] flagsB = flags.finalise();
            buf.writeBytes(flagsB);
            buf.writeInt(msg.nodes.size());
            for (Address addr : msg.nodes) {
                AddressSerializer.INSTANCE.toBinary(addr, buf);
            }
            if (msg.schemaData == null) {
                buf.writeInt(-1);
                return;
            }
            buf.writeInt(msg.schemaData.length);
            buf.writeBytes(msg.schemaData);
            return;
        }
        if (o instanceof Schema.CreateReq) {
            Schema.CreateReq msg = (Schema.CreateReq) o;
            MessageSerializationUtil.msgToBinary(msg, buf, SCHEMA[0], SCHEMA[1]);
            BitBuffer flags = BitBuffer.create(CREATE);
            byte[] flagsB = flags.finalise();
            buf.writeBytes(flagsB);
            SchemaData.serialiseSchema(buf, ByteBuffer.wrap(new byte[0]), msg.name, msg.metaData);
            return;
        }
        if (o instanceof Schema.DropReq) {
            Schema.DropReq msg = (Schema.DropReq) o;
            MessageSerializationUtil.msgToBinary(msg, buf, SCHEMA[0], SCHEMA[1]);
            BitBuffer flags = BitBuffer.create(DROP);
            byte[] flagsB = flags.finalise();
            buf.writeBytes(flagsB);
            byte[] nameB = msg.name.getBytes(SchemaData.CHARSET);
            buf.writeInt(nameB.length);
            buf.writeBytes(nameB);
            return;
        }
        if (o instanceof Schema.Response) {
            Schema.Response msg = (Schema.Response) o;
            MessageSerializationUtil.msgToBinary(msg, buf, SCHEMA[0], SCHEMA[1]);
            BitBuffer flags = BitBuffer.create(RESP);
            flags.write(msg.success);
            byte[] flagsB = flags.finalise();
            buf.writeBytes(flagsB);
            byte[] nameB = msg.name.getBytes(SchemaData.CHARSET);
            buf.writeInt(nameB.length);
            buf.writeBytes(nameB);
            if (msg.id == null) {
                buf.writeInt(-1);
            } else {
                buf.writeInt(msg.id.length);
                buf.writeBytes(msg.id);
            }
            if (msg.msg == null) {
                buf.writeInt(-1);
            } else {
                byte[] msgB = msg.msg.getBytes(SchemaData.CHARSET);
                buf.writeInt(msgB.length);
                buf.writeBytes(msgB);
            }
            return;
        }
        if (o instanceof LUTOutdated) {
            LUTOutdated msg = (LUTOutdated) o;
            MessageSerializationUtil.msgToBinary(msg, buf, LUT[0], LUT[1]);
            BitBuffer flags = BitBuffer.create(LOUTDATED);
            byte[] flagsB = flags.finalise();
            buf.writeBytes(flagsB);
            buf.writeLong(msg.newerlutversion);
            return;
        }
        if (o instanceof LUTPart) {
            LUTPart msg = (LUTPart) o;
            MessageSerializationUtil.msgToBinary(msg, buf, LUT[0], LUT[1]);
            BitBuffer flags = BitBuffer.create(LPART);
            byte[] flagsB = flags.finalise();
            buf.writeBytes(flagsB);
            msg.serialiseContent(buf);
            return;
        }
        LOG.warn("Couldn't serialize {}: {}", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        MessageFields fields = MessageSerializationUtil.msgFromBinary(buf);
        if (matches(fields, FORWARD)) {
            Key key = CustomSerialisers.deserialiseKey(buf);
            Forwardable msg = (Forwardable) Serializers.fromBinary(buf, Optional.absent());
            return new ForwardMessage(fields.src, fields.dst, fields.orig, fields.proto, key, msg);
        }
        if (matches(fields, SAMPLE)) {
            byte[] flagsB = new byte[1];
            buf.readBytes(flagsB);
            boolean[] flags = BitBuffer.extract(8, flagsB);
            if (matches(flags, SREQ)) {
                int n = buf.readInt();
                long lutversion = buf.readLong();
                return new SampleRequest(fields.src, fields.dst, n, flags[2], flags[3], lutversion);
            }
            if (matches(flags, SRESP)) {
                int size = buf.readInt();
                ImmutableSet.Builder<Address> builder = ImmutableSet.builder();
                for (int i = 0; i < size; i++) {
                    Address addr = (Address) AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
                    builder.add(addr);
                }
                int schemaL = buf.readInt();
                if (schemaL < 0) {
                    return new Sample(fields.src, fields.dst, builder.build(), null);
                }
                byte[] schemaData = new byte[schemaL];
                buf.readBytes(schemaData);
                return new Sample(fields.src, fields.dst, builder.build(), schemaData);
            }
        }
        if (matches(fields, SCHEMA)) {
            byte[] flagsB = new byte[1];
            buf.readBytes(flagsB);
            boolean[] flags = BitBuffer.extract(8, flagsB);
            if (matches(flags, CREATE)) {
                SingleSchema schema = deserialiseSchema(buf);
                return new Schema.CreateReq(fields.src, fields.dst, fields.orig, schema.name, schema.meta);
            }
            if (matches(flags, DROP)) {
                int nameL = buf.readInt();
                byte[] nameB = new byte[nameL];
                buf.readBytes(nameB);
                String name = new String(nameB, SchemaData.CHARSET);
                return new Schema.DropReq(fields.src, fields.dst, fields.orig, name);
            }
            if (matches(flags, RESP)) {
                int nameL = buf.readInt();
                byte[] nameB = new byte[nameL];
                buf.readBytes(nameB);
                String name = new String(nameB, SchemaData.CHARSET);
                int idL = buf.readInt();
                byte[] id = null;
                if (idL >= 0) {
                    id = new byte[idL];
                    buf.readBytes(id);
                }
                int msgL = buf.readInt();
                String msg = null;
                if (msgL >= 0) {
                    byte[] msgB = new byte[msgL];
                    buf.readBytes(msgB);
                    msg = new String(msgB, SchemaData.CHARSET);
                }
                return new Schema.Response(fields.src, fields.dst, name, id, flags[2], msg);
            }
        }
        if (matches(fields, LUT)) {
            byte[] flagsB = new byte[1];
            buf.readBytes(flagsB);
            boolean[] flags = BitBuffer.extract(8, flagsB);
            if (matches(flags, LOUTDATED)) {
                long newerlutversion = buf.readLong();
                return new LUTOutdated(fields.src, fields.dst, fields.orig, newerlutversion);
            }
            if (matches(flags, LPART)) {
                return LUTPart.deserialiseContent(buf, fields);
            }
        }
        LOG.warn("Don't know how to deserialise fields: {}", fields);
        return null;
    }

    private boolean matches(MessageFields fields, Boolean[] type) {
        return (fields.flag1 == type[0]) && (fields.flag2 == type[1]);
    }

    private boolean matches(boolean[] flags, Boolean[] type) {
        return (flags[0] == type[0]) && (flags[1] == type[1]);
    }

}
