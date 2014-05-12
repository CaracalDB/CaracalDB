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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
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
    public static final Key NULL_KEY = new Key(new byte[0]);
    public static final Key ZERO_KEY = new Key(new byte[] {ZERO});
    public static final Key INF = new Inf();
    private static final Charset charset = Charset.forName("UTF-8");
    private final byte[] data; // Don't write into it either! 

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
    
    public KeyBuilder append(Key k) {
        if(k.equals(Key.NULL_KEY)) {
            return new KeyBuilder(data);
        }
        return append(k.data);
    }
    
    public KeyBuilder append(byte[] k) {
        KeyBuilder kb = new KeyBuilder(data);
        kb.append(k);
        return kb;
    }
    
    public KeyBuilder prepend(Key k) {
        if(k.equals(Key.NULL_KEY)) {
            return new KeyBuilder(data);
        }
        return prepend(k.data);
    }
    
    public KeyBuilder prepend(byte[] k) {
        KeyBuilder kb = new KeyBuilder(data);
        kb.prepend(k);
        return kb;
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
    
    /**
     * Does constant key length increment.
     * 
     * That is (00 00 00 00).inc() = (00 00 00 01) and so on.
     * 
     * Exceptions: 
     *      (FF FF FF FF).inc() = Inf for any key length.
     *      ( ).inc() = (00)
     * 
     * @return The key that is an increment of the current one with constant length.
     */
    public Key inc() {
        if (data.length == 0) {
            return new Key((byte)0);
        }
        byte[] newData  = Arrays.copyOf(data, data.length);
        for (int i = newData.length-1; i >= 0; i--) {
            int oldVal = UnsignedBytes.toInt(newData[i]);
            //System.out.println(oldVal + " != " + UnsignedBytes.MAX_VALUE);
            if (oldVal != BYTE_KEY_SIZE) {
                newData[i] = UnsignedBytes.checkedCast(oldVal+1);
                return new Key(newData);
            } else {
                newData[i] = 0;
            }
        }
        return Key.INF;
    }

    @Override
    public int compareTo(Key that) {
        if (that instanceof Inf) {
            return Integer.MIN_VALUE;
        }
        return compare(this.data, that.data);
    }

    public static int compare(byte[] key1, byte[] key2) {
        if (key1 == null) {
            if (key2 == null) {
                return 0;
            } else {
                return Integer.MIN_VALUE;
            }
        }
        if (key2 == null) {
            return Integer.MAX_VALUE;
        }
        
        return COMP.compare(key1, key2);
    }

    /**
     * @param that can be Key or byte[] representation of a key
     */
    @Override
    public boolean equals(Object that) {
        if (that instanceof Key) {
            Key k = (Key) that;
            return this.compareTo(k) == 0;
        } else if (that instanceof byte[]) {
            byte[] k = (byte[]) that;
            return COMP.compare(k,this.getArray()) == 0;
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
    
    public boolean leq(byte[] k) {
        return COMP.compare(data, k) <= 0;
    }

    public boolean less(Key k) {
        return compareTo(k) < 0;
    }
    
    public boolean less(byte[] k) {
        return COMP.compare(data, k) < 0;
    }

    public boolean geq(Key k) {
        return compareTo(k) >= 0;
    }
    
    public boolean geq(byte[] k) {
        return COMP.compare(data, k) >= 0;
    }

    public boolean greater(Key k) {
        return compareTo(k) > 0;
    }
    
    public boolean greater(byte[] k) {
        return COMP.compare(data,k) > 0;
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
        if(hex.equals("")) {
            return Key.NULL_KEY;
        }
        String[] byteBlocks = hex.split("\\s");
        byte[] bytes = new byte[byteBlocks.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = UnsignedBytes.parseUnsignedByte(byteBlocks[i], 16);
        }
        return new Key(bytes);
    }

    public static class Inf extends Key {

        private Inf() {
            super((byte[]) null);
        }
        
        @Override
        public Key inc() {
            return this; // Inf++ == Inf
        }

        @Override
        public int compareTo(Key that) {
            if (that instanceof Inf) {
                return 0;
            }
            return Integer.MAX_VALUE;
        }
        
        @Override
        public String toString() {
            return "∞";
        }
    }
    
    public static class KeyBuilder {
        private int numBytes;
        private LinkedList<byte[]> bytes;
        
        public KeyBuilder(byte[] start) {
            assert(start.length > 0);
            bytes = new LinkedList<byte[]>();
            bytes.push(start);
            numBytes = start.length;
        }
        
        public KeyBuilder append(byte[] k) {
            bytes.add(k);
            numBytes += k.length;
            return this;
        }
        
        public KeyBuilder append(Key k) {
            return append(k.data);
        }
        
        public KeyBuilder prepend(byte[] k) {
            bytes.push(k);
            numBytes += k.length;
            return this;
        }
        
        public KeyBuilder prepend(Key k) {
            return prepend(k.data);
        }
        
        public Key get() {
            byte[] data = new byte[numBytes];
            int pointer = 0;
            while (!bytes.isEmpty()) {
                byte[] k = bytes.pop();
                System.arraycopy(k, 0, data, pointer, k.length);
                pointer += k.length;
            }
            return new Key(data);
        }
    }
}