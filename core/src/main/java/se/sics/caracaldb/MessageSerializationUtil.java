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
package se.sics.caracaldb;

import com.google.common.base.Optional;
import com.larskroll.common.BitBuffer;
import io.netty.buffer.ByteBuf;
import se.sics.kompics.network.Transport;

/**
 *
 * @author lkroll
 */
public abstract class MessageSerializationUtil {

    public static void msgToBinary(BaseMessage msg, ByteBuf buf, boolean flag1, boolean flag2) {
        // Flags 1byte
        boolean sourceEqOrigin = msg.getSource().equals(msg.getOrigin());
        BitBuffer bbuf = BitBuffer.create(flag1, flag2, // good that a byte has so many bits... can compress it more if more protocols are necessary
                sourceEqOrigin,
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
        if (!sourceEqOrigin) {
            AddressSerializer.INSTANCE.toBinary(msg.getOrigin(), buf);
        }
    }
    
    public static void msgToBinary(BaseDataMessage msg, ByteBuf buf, boolean flag1, boolean flag2) {
        // Flags 1byte
        boolean sourceEqOrigin = msg.getSource().equals(msg.getOrigin());
        BitBuffer bbuf = BitBuffer.create(flag1, flag2, // good that a byte has so many bits... can compress it more if more protocols are necessary
                sourceEqOrigin,
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
        if (!sourceEqOrigin) {
            AddressSerializer.INSTANCE.toBinary(msg.getOrigin(), buf);
        }
    }

    public static MessageFields msgFromBinary(ByteBuf buf) {
        MessageFields fields = new MessageFields();

        byte[] flagB = new byte[1];
        buf.readBytes(flagB);
        boolean[] flags = BitBuffer.extract(8, flagB);
        fields.flag1 = flags[0];
        fields.flag2 = flags[1];
        boolean sourceEqOrigin = flags[2];
        if (flags[3]) {
            fields.proto = Transport.UDP;
        }
        if (flags[4]) {
            fields.proto = Transport.TCP;
        }
        if (flags[5]) {
            fields.proto = Transport.MULTICAST_UDP;
        }
        if (flags[6]) {
            fields.proto = Transport.UDT;
        }
        if (flags[7]) {
            fields.proto = Transport.LEDBAT;
        }

        // Addresses
        fields.src = (Address) AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        fields.dst = (Address) AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        fields.orig = fields.src;
        if (!sourceEqOrigin) {
            fields.orig = (Address) AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        }

        return fields;
    }

    public static class MessageFields {

        public Address src;
        public Address dst;
        public Address orig;
        public Transport proto;
        public boolean flag1;
        public boolean flag2;
    }

}
