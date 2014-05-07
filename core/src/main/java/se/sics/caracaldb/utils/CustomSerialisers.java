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
package se.sics.caracaldb.utils;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.primitives.UnsignedBytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.BitBuffer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class CustomSerialisers {
    /*
     * Custom Address serialisation to save some space
     */

    public static byte[] serialiseAddress(Address addr) {
        ByteBuf buf = Unpooled.buffer();
        SpecialSerializers.AddressSerializer.INSTANCE.toBinary(addr, buf);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        buf.release();
        return bytes;
    }

    public static Address deserialiseAddress(byte[] data) {
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        Address addr = (Address) SpecialSerializers.AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        buf.release();
        return addr;
    }

    public static void serialiseKey(Key key, ByteBuf w) {
        byte[] id = key.getArray();
        BitBuffer bbuf = BitBuffer.create((key instanceof Key.Inf), (id == null));
        boolean byteFlag = (id != null) && (id.length <= Key.BYTE_KEY_SIZE);
        bbuf.write(byteFlag);
        byte[] flags = bbuf.finalise();
        w.writeBytes(flags);
        if (id != null) {
            if (byteFlag) {
                w.writeByte(UnsignedBytes.checkedCast(id.length));
            } else {
                w.writeInt(id.length);
            }
            w.writeBytes(id);
        }
    }

    public static void serialiseKey(byte[] key, ByteBuf w) {
        serialiseKey(new Key(key), w);
    }

    public static byte[] serialiseKey(Key key) {
        ByteBuf buf = Unpooled.buffer();
        serialiseKey(key, buf);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        buf.release();
        return bytes;
    }

    public static byte[] serialiseKey(byte[] key) {
        return serialiseKey(new Key(key));
    }

    public static Key deserialiseKey(ByteBuf r) {
        byte[] flagBytes = new byte[1];
        r.readBytes(flagBytes);
        boolean[] flags = BitBuffer.extract(3, flagBytes);
        boolean infFlag = flags[0];
        boolean nullFlag = flags[1];
        boolean byteFlag = flags[2];
        if (infFlag) {
            return Key.INF;
        }
        if (nullFlag) {
            return new Key((byte[]) null);
        }
        int keySize;
        if (byteFlag) {
            keySize = UnsignedBytes.toInt(r.readByte());
        } else {
            keySize = r.readInt();
        }
        byte[] id;
        id = new byte[keySize];
        r.readBytes(id);
        return new Key(id);
    }
    
    public static Key deserialiseKey(byte[] r) {
        ByteBuf buf = Unpooled.wrappedBuffer(r);
        Key k = deserialiseKey(buf);
        buf.release();
        return k;
    }
    
    public static void serialiseKeyRange(KeyRange range, ByteBuf w) {
        BitBuffer bbuf = BitBuffer.create(range.beginBound == KeyRange.Bound.CLOSED,
                range.endBound == KeyRange.Bound.CLOSED);
        byte[] flags = bbuf.finalise();
        w.writeBytes(flags);
        serialiseKey(range.begin, w);
        serialiseKey(range.end, w);
    }
    
    public static byte[] serialiseKeyRange(KeyRange range) {
        ByteBuf buf = Unpooled.buffer();
        serialiseKeyRange(range, buf);
        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);
        buf.release();
        return bytes;
    }
    
    public static KeyRange deserialiseKeyRange(ByteBuf r) {
        byte[] flags = new byte[1];
        r.readBytes(flags);
        boolean[] bounds = BitBuffer.extract(2, flags);
        KeyRange.Bound beginBound = bounds[0] ? KeyRange.Bound.CLOSED : KeyRange.Bound.OPEN;
        KeyRange.Bound endBound = bounds[1] ? KeyRange.Bound.CLOSED : KeyRange.Bound.OPEN;
        Key begin = deserialiseKey(r);
        Key end = deserialiseKey(r);
        return new KeyRange(beginBound, begin, end, endBound);
    }
    
    public static KeyRange deserialiseKeyRange(byte[] data) {
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        KeyRange kr = deserialiseKeyRange(buf);
        buf.release();
        return kr;
    }
    
    public static void serialiseView(View view, ByteBuf buf) {
        buf.writeInt(view.id);
        buf.writeInt(view.members.size());
        for (Address addr : view.members) {
            SpecialSerializers.AddressSerializer.INSTANCE.toBinary(addr, buf);
        }
    }
    
    public static View deserialiseView(ByteBuf buf) {
        int id = buf.readInt();
        int memsize = buf.readInt();
        ImmutableSortedSet.Builder<Address> addrs = ImmutableSortedSet.naturalOrder();
        for (int i = 0; i < memsize; i++) {
            Address addr = (Address) SpecialSerializers.AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            addrs.add(addr);
        }
        return new View(addrs.build(), id);
    }
    
//    public static class BitBuffer {
//
//        private static final int ZERO = 0;
//        private static final int[] POS = {1, 2, 4, 8, 16, 32, 64, 128};
//
//        private final ArrayList<Boolean> buffer = new ArrayList<Boolean>();
//
//        private BitBuffer() {
//        }
//
//        public static BitBuffer create(Boolean... args) {
//            BitBuffer b = new BitBuffer();
//            b.buffer.addAll(Arrays.asList(args));
//            return b;
//        }
//
//        public BitBuffer write(Boolean... args) {
//            buffer.addAll(Arrays.asList(args));
//            return this;
//        }
//
//        public byte[] finalise() {
//            int numBytes = (int) Math.ceil(((double) buffer.size()) / 8.0);
//            byte[] bytes = new byte[numBytes];
//            for (int i = 0; i < numBytes; i++) {
//                int b = ZERO;
//                for (int j = 0; j < 8; j++) {
//                    int pos = i * 8 + j;
//                    if (buffer.size() > pos) {
//                        if (buffer.get(pos)) {
//                            b = b ^ POS[j];
//                        }
//                    }
//                }
//                bytes[i] = UnsignedBytes.checkedCast(b);
//            }
//            return bytes;
//        }
//
//        public static boolean[] extract(int numValues, byte[] bytes) {
//            assert (((int) Math.ceil(((double) numValues) / 8.0)) <= bytes.length);
//
//            boolean[] output = new boolean[numValues];
//            for (int i = 0; i < bytes.length; i++) {
//                int b = bytes[i];
//                for (int j = 0; j < 8; j++) {
//                    int pos = i * 8 + j;
//                    if (pos >= numValues) {
//                        return output;
//                    }
//                    output[pos] = ((b & POS[j]) != 0);
//                }
//            }
//
//            return output;
//        }
//    }
}
