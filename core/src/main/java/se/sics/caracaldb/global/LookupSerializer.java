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
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 *
 * @author lkroll
 */
public class LookupSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(LookupSerializer.class);

    private static final Boolean[] FORWARD = new Boolean[]{false, false};
    private static final Boolean[] REQUEST = new Boolean[]{true, false};
    private static final Boolean[] SAMPLE = new Boolean[]{true, true};

    @Override
    public int identifier() {
        return CoreSerializer.LOOKUP.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof ForwardMessage) {
            ForwardMessage msg = (ForwardMessage) o;
            SpecialSerializers.MessageSerializationUtil.msgToBinary(msg, buf, FORWARD[0], FORWARD[1]);
            CustomSerialisers.serialiseKey(msg.forwardTo, buf);
            Serializers.toBinary(msg.msg, buf);
            return;
        }
        if (o instanceof SampleRequest) {
            SampleRequest msg = (SampleRequest) o;
            SpecialSerializers.MessageSerializationUtil.msgToBinary(msg, buf, REQUEST[0], REQUEST[1]);
            buf.writeInt(msg.n);
            return;
        }
        if (o instanceof Sample) {
            Sample msg = (Sample) o;
            SpecialSerializers.MessageSerializationUtil.msgToBinary(msg, buf, SAMPLE[0], SAMPLE[1]);
            buf.writeInt(msg.nodes.size());
            msg.nodes.forEach((addr) -> {
                SpecialSerializers.AddressSerializer.INSTANCE.toBinary(addr, buf);
            });
            return;
        }
        LOG.warn("Couldn't serialize {}: {}", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Class> hint) {
        MessageFields fields = SpecialSerializers.MessageSerializationUtil.msgFromBinary(buf);
        if (matches(fields, FORWARD)) {
            Key key = CustomSerialisers.deserialiseKey(buf);
            Forwardable msg = (Forwardable) Serializers.fromBinary(buf, Optional.absent());
            return new ForwardMessage(fields.src, fields.dst, fields.orig, fields.proto, key, msg);
        }
        if (matches(fields, REQUEST)) {
            int n = buf.readInt();
            return new SampleRequest(fields.src, fields.dst, n);
        }
        if (matches(fields, SAMPLE)) {
            int size = buf.readInt();
            ImmutableSet.Builder<Address> builder = ImmutableSet.builder();
            for (int i = 0; i < size; i++) {
                Address addr = (Address) SpecialSerializers.AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
                builder.add(addr);
            }
            return new Sample(fields.src, fields.dst, builder.build());
        }
        LOG.warn("Don't know how to deserialise fields: {}", fields);
        return null;
    }
    
    private boolean matches(MessageFields fields, Boolean[] type) {
        return (fields.flag1 == type[0]) && (fields.flag2 == type[1]);
    }

}
