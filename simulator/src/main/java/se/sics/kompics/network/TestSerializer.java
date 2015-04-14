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
import com.google.common.primitives.Ints;
import com.larskroll.common.BitBuffer;
import io.netty.buffer.ByteBuf;
import java.net.InetAddress;
import java.net.UnknownHostException;
import se.sics.kompics.network.TestSerializer.MessageSerializationUtil.MessageFields;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.network.test.TestAddress;

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
                MessageSerializationUtil.msgToBinary(dmsg, buf, false, false);
                buf.writeLong(dmsg.id);
                int dataL = dmsg.data.length;
                buf.writeInt(dataL);
                buf.writeBytes(dmsg.data);
            } else if (o instanceof DataMessage.Ack) {
                DataMessage.Ack amsg = (DataMessage.Ack) o;
                MessageSerializationUtil.msgToBinary(amsg, buf, true, false);
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
        MessageFields msg = MessageSerializationUtil.msgFromBinary(buf);
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

    
    public static abstract class MessageSerializationUtil {

        public static void msgToBinary(TestMessage msg, ByteBuf buf, boolean flag1, boolean flag2) {
            BitBuffer bbuf = BitBuffer.create(flag1, flag2, // good that a byte has so many bits... can compress it more if more protocols are necessary
                    msg.getProtocol() == Transport.UDP,
                    msg.getProtocol() == Transport.TCP,
                    msg.getProtocol() == Transport.MULTICAST_UDP,
                    msg.getProtocol() == Transport.UDT,
                    msg.getProtocol() == Transport.LEDBAT);
            byte[] bbufb = bbuf.finalise();
            buf.writeBytes(bbufb);
            // Addresses
            AddressSerializer.INSTANCE.toBinary(msg.getSource(), buf);
            AddressSerializer.INSTANCE.toBinary(msg.getDestination(), buf);
        }

        public static MessageFields msgFromBinary(ByteBuf buf) {
            MessageFields fields = new MessageFields();

            byte[] flagB = new byte[1];
            buf.readBytes(flagB);
            boolean[] flags = BitBuffer.extract(8, flagB);
            fields.flag1 = flags[0];
            fields.flag2 = flags[1];
            if (flags[2]) {
                fields.proto = Transport.UDP;
            }
            if (flags[3]) {
                fields.proto = Transport.TCP;
            }
            if (flags[4]) {
                fields.proto = Transport.MULTICAST_UDP;
            }
            if (flags[5]) {
                fields.proto = Transport.UDT;
            }
            if (flags[6]) {
                fields.proto = Transport.LEDBAT;
            }

            // Addresses
            fields.src = (TestAddress) AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            fields.dst = (TestAddress) AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            fields.orig = fields.src;

            return fields;
        }

        public static class MessageFields {

            public TestAddress src;
            public TestAddress dst;
            public TestAddress orig;
            public Transport proto;
            public boolean flag1;
            public boolean flag2;
        }
    }
    
    public static class AddressSerializer implements Serializer {

        public static final int BYTE_KEY_SIZE = 255;
        public static final int INT_BYTE_SIZE = Integer.SIZE / 8;
        public static final AddressSerializer INSTANCE = new AddressSerializer();

        @Override
        public int identifier() {
            return 2;
        }

        @Override
        public void toBinary(Object o, ByteBuf buf) {
            TestAddress addr = (TestAddress) o;
            if (addr == null) {
                buf.writeInt(0); //simply put four 0 bytes since 0.0.0.0 is not a valid host ip
                return;
            }

            buf.writeBytes(addr.getIp().getAddress());
            // Write ports as 2 bytes instead of 4
            byte[] portBytes = Ints.toByteArray(addr.getPort());
            buf.writeByte(portBytes[2]);
            buf.writeByte(portBytes[3]);
        }

        @Override
        public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
            byte[] ipBytes = new byte[4];
            buf.readBytes(ipBytes);
            if ((ipBytes[0] == 0) && (ipBytes[1] == 0) && (ipBytes[2] == 0) && (ipBytes[3] == 0)) {
                return null; // IP 0.0.0.0 is not valid but null Address encoding
            }
            InetAddress ip;
            try {
                ip = InetAddress.getByAddress(ipBytes);
            } catch (UnknownHostException ex) {
                Serializers.LOG.error("AddressSerializer: Could not create InetAddress.", ex);
                return null;
            }
            byte portUpper = buf.readByte();
            byte portLower = buf.readByte();
            int port = Ints.fromBytes((byte) 0, (byte) 0, portUpper, portLower);
            

            return new TestAddress(ip, port);
        }

    }
}
