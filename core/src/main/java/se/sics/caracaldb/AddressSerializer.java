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
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import com.larskroll.common.BitBuffer;
import io.netty.buffer.ByteBuf;
import java.net.InetAddress;
import java.net.UnknownHostException;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;

/**
 *
 * @author lkroll
 */
public class AddressSerializer implements Serializer {

    public static final int BYTE_KEY_SIZE = 255;
    public static final int INT_BYTE_SIZE = Integer.SIZE / 8;
    public static final AddressSerializer INSTANCE = new AddressSerializer();

    @Override
    public int identifier() {
        return 2;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        Address addr = (Address) o;
        if (addr == null) {
            buf.writeInt(0); //simply put four 0 bytes since 0.0.0.0 is not a valid host ip
            return;
        }
//            int length = 6 + 4 + addr.getId().length;
//            int code = buf.ensureWritable(length, true);
//            if (code == 1 || code == 3) {
//                Serializers.LOG.error("AddressSerializer: Not enough space left on buffer to serialize " + length + " bytes.");
//                return;
//            }

        buf.writeBytes(addr.getIp().getAddress());
        // Write ports as 2 bytes instead of 4
        byte[] portBytes = Ints.toByteArray(addr.getPort());
        buf.writeByte(portBytes[2]);
        buf.writeByte(portBytes[3]);
        // Id
        byte[] id = addr.getId();
        BitBuffer bbuf = BitBuffer.create(false, (id == null));
        boolean byteFlag = (id != null) && (id.length <= BYTE_KEY_SIZE);
        bbuf.write(byteFlag);
        byte[] flags = bbuf.finalise();
        buf.writeBytes(flags);
        if (id != null) {
            if (byteFlag) {
                buf.writeByte(UnsignedBytes.checkedCast(id.length));
            } else {
                buf.writeInt(id.length);
            }
            buf.writeBytes(id);
        }
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

        byte[] flagBytes = new byte[1];
        buf.readBytes(flagBytes);
        boolean[] flags = BitBuffer.extract(3, flagBytes);
        boolean infFlag = flags[0];
        boolean nullFlag = flags[1];
        boolean byteFlag = flags[2];

        byte[] id;

        if (nullFlag || infFlag) {
            id = null;
            return new Address(ip, port, id);
        }
        int keySize;
        if (byteFlag) {
            keySize = UnsignedBytes.toInt(buf.readByte());
        } else {
            keySize = buf.readInt();
        }
        id = new byte[keySize];
        buf.readBytes(id);

        return new Address(ip, port, id);
    }

}
