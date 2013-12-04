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

import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class CustomSerialisers {
    /*
     * Custom Address serialisation to save some space
     */

    public static void serialiseAddress(Address addr, DataOutputStream w) throws IOException {
        if (addr == null) {
            w.writeInt(0); //simply put four 0 bytes since 0.0.0.0 is not a valid host ip
            return;
        }
        w.write(addr.getIp().getAddress());
        // Write ports as 2 bytes instead of 4
        byte[] portBytes = Ints.toByteArray(addr.getPort());
        w.writeByte(portBytes[2]);
        w.writeByte(portBytes[3]);
        serialiseKey(addr.getId(), w);

    }

    public static byte[] serialiseAddress(Address addr) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            serialiseAddress(addr, w);
            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static Address deserialiseAddress(DataInputStream r) throws IOException {
        byte[] ipBytes = new byte[4];
        if (r.read(ipBytes) != 4) {
            throw new IOException("Incomplete dataset!");
        }
        if ((ipBytes[0] == 0) && (ipBytes[1] == 0) && (ipBytes[2] == 0) && (ipBytes[3] == 0)) {
            return null; // IP 0.0.0.0 is not valid but null Address encoding
        }
        InetAddress ip = InetAddress.getByAddress(ipBytes);
        byte portUpper = r.readByte();
        byte portLower = r.readByte();
        int port = Ints.fromBytes((byte) 0, (byte) 0, portUpper, portLower);
        Key id = deserialiseKey(r);
        return new Address(ip, port, id.getArray());
    }

    public static void serialiseKey(Key key, DataOutputStream w) throws IOException {
        byte[] id = key.getArray();
        BitBuffer bbuf = BitBuffer.create((key instanceof Key.Inf), (id == null));
        boolean byteFlag = (id != null) && (id.length <= Key.BYTE_KEY_SIZE);
        bbuf.write(byteFlag);
        byte[] flags = bbuf.finalise();
        w.write(flags);
        if (id != null) {
            if (byteFlag) {
                w.writeByte(UnsignedBytes.checkedCast(id.length));
            } else {
                w.writeInt(id.length);
            }
            w.write(id);
        }
    }

    public static void serialiseKey(byte[] key, DataOutputStream w) throws IOException {
        serialiseKey(new Key(key), w);
    }

    public static byte[] serialiseKey(Key key) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            serialiseKey(key, w);
            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static byte[] serialiseKey(byte[] key) throws IOException {
        return serialiseKey(new Key(key));
    }

    public static Key deserialiseKey(DataInputStream r) throws IOException {
        byte[] flagBytes = new byte[1];
        r.read(flagBytes);
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
        if (r.read(id) != keySize) {
            throw new IOException("Incomplete dataset!");
        }
        return new Key(id);
    }
    
    public static void serialiseKeyRange(KeyRange range, DataOutputStream w) throws IOException {
        BitBuffer bbuf = new BitBuffer();
        bbuf.write(range.beginBound == KeyRange.Bound.CLOSED);
        bbuf.write(range.endBound == KeyRange.Bound.CLOSED);
        byte[] flags = bbuf.finalise();
        w.write(flags);
        serialiseKey(range.begin, w);
        serialiseKey(range.end, w);
    }
    
    public static byte[] serialiseKeyRange(KeyRange range) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            serialiseKeyRange(range, w);
            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }
    
    public static KeyRange deserialiseKeyRange(DataInputStream r) throws IOException {
        byte[] flags = new byte[1];
        r.read(flags);
        boolean[] bounds = BitBuffer.extract(2, flags);
        KeyRange.Bound beginBound = bounds[0] ? KeyRange.Bound.CLOSED : KeyRange.Bound.OPEN;
        KeyRange.Bound endBound = bounds[1] ? KeyRange.Bound.CLOSED : KeyRange.Bound.OPEN;
        Key begin = deserialiseKey(r);
        Key end = deserialiseKey(r);
        return new KeyRange(beginBound, begin, end, endBound);
    }
    
    public static class BitBuffer {

        private static final int ZERO = 0;
        private static final int[] POS = {1, 2, 4, 8, 16, 32, 64, 128};

        private final ArrayList<Boolean> buffer = new ArrayList<Boolean>();

        private BitBuffer() {
        }

        public static BitBuffer create(Boolean... args) {
            BitBuffer b = new BitBuffer();
            b.buffer.addAll(Arrays.asList(args));
            return b;
        }

        public BitBuffer write(Boolean... args) {
            buffer.addAll(Arrays.asList(args));
            return this;
        }

        public byte[] finalise() {
            int numBytes = (int) Math.ceil(((double) buffer.size()) / 8.0);
            byte[] bytes = new byte[numBytes];
            for (int i = 0; i < numBytes; i++) {
                int b = ZERO;
                for (int j = 0; j < 8; j++) {
                    int pos = i * 8 + j;
                    if (buffer.size() > pos) {
                        if (buffer.get(pos)) {
                            b = b ^ POS[j];
                        }
                    }
                }
                bytes[i] = UnsignedBytes.checkedCast(b);
            }
            return bytes;
        }

        public static boolean[] extract(int numValues, byte[] bytes) {
            assert (((int) Math.ceil(((double) numValues) / 8.0)) <= bytes.length);

            boolean[] output = new boolean[numValues];
            for (int i = 0; i < bytes.length; i++) {
                int b = bytes[i];
                for (int j = 0; j < 8; j++) {
                    int pos = i * 8 + j;
                    if (pos >= numValues) {
                        return output;
                    }
                    output[pos] = ((b & POS[j]) != 0);
                }
            }

            return output;
        }
    }
}
