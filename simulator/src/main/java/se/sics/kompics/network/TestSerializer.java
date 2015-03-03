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
package se.sics.kompics.network;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 *
 * @author lkroll
 */
public class TestSerializer implements Serializer {

    @Override
    public int identifier() {
        return 100;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof TestMessage) {
            if (o instanceof DataMessage) {
                DataMessage dmsg = (DataMessage) o;
                SpecialSerializers.MessageSerializationUtil.msgToBinary(dmsg, buf, false, false);
                buf.writeLong(dmsg.id);
                int dataL = dmsg.data.length;
                buf.writeInt(dataL);
                buf.writeBytes(dmsg.data);
            } else if (o instanceof DataMessage.Ack) {
                DataMessage.Ack amsg = (DataMessage.Ack) o;
                SpecialSerializers.MessageSerializationUtil.msgToBinary(amsg, buf, true, false);
                buf.writeLong(amsg.id);
            } else {
                throw new RuntimeException("No serializer for class: " + o.getClass().getName());
            }
        } else {
            throw new RuntimeException("No serializer for class: " + o.getClass().getName());
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        MessageFields msg = SpecialSerializers.MessageSerializationUtil.msgFromBinary(buf);
        long id = buf.readLong();
        if (!msg.flag1) { // DataMessage
            int dataL = buf.readInt();
            byte[] data = new byte[dataL];
            buf.readBytes(data);
            return new DataMessage(msg.src, msg.dst, msg.proto, id, data);
        } else { // Ack
            return new DataMessage.Ack(msg.src, msg.dst, msg.proto, id);
        }
    }

}
