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

import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedInts;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import se.sics.kompics.address.IdUtils;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Key implements Comparable<Key> {

    public static final int BYTE_KEY_SIZE = 255;
    public static final int INT_BYTE_SIZE = Integer.SIZE / 8;
    public static final Comparator<byte[]> COMP = UnsignedBytes.lexicographicalComparator();
    private static final byte ZERO = 0;
    public static final Key ZERO_KEY = new Key(new byte[0]);
    public static final Key INF = new Inf();
    private static final Charset charset = Charset.forName("UTF-8");
    private byte[] data;

    public Key(byte key) {
        data = new byte[]{key};
    }

    public Key(byte[] key) {
        data = key;
    }

    public Key(ByteBuffer buffer) {
        data = buffer.array();
    }

    public Key(int... ints) {
        ByteBuffer buf = ByteBuffer.allocate(ints.length * INT_BYTE_SIZE);
        for (Integer i : ints) {
            buf.putInt(i);
        }
        data = buf.array();
//        data = new byte[ints.length*INT_BYTE_SIZE];
//        buf.flip();
//        buf.get(data);
    }

    public Key(String str) {
        data = str.getBytes(charset);
    }

    public Key(UnsignedInteger num) {
        this(num.intValue());
    }

    public ByteBuffer getWrapper() {
        return ByteBuffer.wrap(data);
    }

    public byte[] getArray() {
        return data;
    }

    public int getFirstByte() {
        return Ints.fromBytes(ZERO, ZERO, ZERO, data[0]);
    }

    public int getKeySize() {
        return data.length;
    }

    public byte getByteKeySize() {
        return UnsignedBytes.checkedCast(data.length);
    }

    public boolean isByteLength() {
        return data.length <= BYTE_KEY_SIZE;
    }

    @Override
    public int compareTo(Key that) {
        if (that instanceof Inf) {
            return Integer.MIN_VALUE;
        }
        return COMP.compare(this.data, that.data);
    }

    public static int compare(byte[] key1, byte[] key2) {
        return COMP.compare(key1, key2);
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof Key) {
            Key k = (Key) that;
            return this.compareTo(k) == 0;
        }
        return false;

    }

    @Override
    public int hashCode() {
        int h = 1;
        for (int i = data.length - 1; i >= 0; i--) {
            h = 31 * h + (int) data[i];
        }
        return h;
    }

    @Override
    public String toString() {
        return IdUtils.printFormat(data);
    }

    // wtb operator overrides in Java -.-
    public boolean leq(Key k) {
        return compareTo(k) <= 0;
    }

    public boolean less(Key k) {
        return compareTo(k) < 0;
    }

    public boolean geq(Key k) {
        return compareTo(k) >= 0;
    }

    public boolean greater(Key k) {
        return compareTo(k) > 0;
    }

    public static boolean leq(byte[] k1, byte[] k2) {
        return compare(k1, k2) <= 0;
    }

    public static boolean less(byte[] k1, byte[] k2) {
        return compare(k1, k2) < 0;
    }

    public static boolean geq(byte[] k1, byte[] k2) {
        return compare(k1, k2) >= 0;
    }

    public static boolean greater(byte[] k1, byte[] k2) {
        return compare(k1, k2) > 0;
    }

    public static Key fromHex(String hex) {
        int num = UnsignedInts.parseUnsignedInt(hex.replaceAll("\\s", ""), 16);
        return new Key(num);
    }

    public static class Inf extends Key {

        private Inf() {
            super((byte[]) null);
        }

        @Override
        public int compareTo(Key that) {
            return Integer.MAX_VALUE;
        }
    }
}