/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb;

import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Comparator;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Key implements Comparable<Key> {

    public static final int BYTE_KEY_SIZE = 255;
    private static final byte ZERO = 0;
    private static final Comparator<byte[]> COMP = UnsignedBytes.lexicographicalComparator();
    private static final Charset charset = Charset.forName("UTF-8");
    
    private byte[] data;

    public Key(byte[] key) {
        data = key;
    }

    public Key(ByteBuffer buffer) {
        data = buffer.array();
    }
    
    public Key(int... ints) {
        ByteBuffer buf = ByteBuffer.allocate(ints.length*Integer.SIZE);
        for (Integer i : ints) {
            buf.putInt(i);
        }
        data = buf.array();
    }
    
    public Key(String str) {
        data = str.getBytes(charset);
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
        return COMP.compare(this.data, that.data);
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
}
