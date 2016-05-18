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
package se.sics.caracaldb.experiment.randomdata;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.UUID;
import se.sics.caracaldb.MessageSerializationUtil;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;

/**
 *
 * @author lkroll
 */
public class RandomDataSerializer implements Serializer {

    private static final boolean[] DATA = new boolean[]{false, false};

    @Override
    public int identifier() {
        return 150;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof RandomDataMessage) {
            RandomDataMessage msg = (RandomDataMessage) o;
            MessageSerializationUtil.msgToBinary(msg, buf, DATA[0], DATA[1]);
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(msg.id, buf);
            buf.writeInt(msg.data.length);
            buf.writeBytes(msg.data);
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        long arrivalTime = System.currentTimeMillis();
        MessageSerializationUtil.MessageFields fields = MessageSerializationUtil.msgFromBinary(buf);
        UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        if (matches(fields, DATA)) {
            int bytes = buf.readInt();
            byte[] data = new byte[bytes];
            buf.readBytes(data);
            return new RandomDataMessage(fields.src, fields.dst, fields.proto, id, data, arrivalTime);
        }
        return null;
    }

    private boolean matches(MessageSerializationUtil.MessageFields fields, boolean[] flags) {
        return fields.flag1 == flags[0] && fields.flag2 == flags[1];
    }

    public static byte[] uuid2Bytes(UUID id) {
        byte[] bytes = new byte[16];
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        buf.clear();
        SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(id, buf);
        return bytes;
    }
    
}
