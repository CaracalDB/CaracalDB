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

/**
 *
 * @author lkroll
 */
public class ByteArrayRef implements Comparable<ByteArrayRef> {

    public final int begin;
    public final int length;
    private final byte[] backingArray;

    public ByteArrayRef(int begin, int length, byte[] backingArray) {
        this.begin = begin;
        this.length = length;
        this.backingArray = backingArray;
    }

    /**
     * Returns the complete backing array (no copy)
     *
     * @return
     */
    public byte[] getBackingArray() {
        return backingArray;
    }

    /**
     * Extracts the referenced part of the backing array into a new byte[]
     *
     * @return
     */
    public byte[] dereference() {
        if (length == 0) {
            return null;
        }
        byte[] data = new byte[length];
        System.arraycopy(backingArray, begin, data, 0, length);
        return data;
    }

    public byte dereference(int i) {
        if (i >= length) {
            throw new IndexOutOfBoundsException("Asked for index " + i + " but length is only " + length);
        }
        return backingArray[begin + i];
    }

    public void assign(int i, byte val) {
        if (i >= length) {
            throw new IndexOutOfBoundsException("Asked for index " + i + " but length is only " + length);
        }
        backingArray[begin + i] = val;
    }

    /* NOTE: This code just asks to be abused such that it circumvents correct 
     (storage layer dependent) multi version usage.
     Hence having is not such a greadt idea after all.

     public ByteArrayRef replaceWith(byte[] newData) {
     if (newData == null) {
     delete();
     }
     int newLength = backingArray.length + (newData.length - this.length);
     byte[] newBack = new byte[newLength];
     System.arraycopy(backingArray, 0, newBack, 0, begin);
     System.arraycopy(newData, 0, newBack, begin, newData.length);
     System.arraycopy(backingArray, begin + length, newBack, begin + newData.length, newLength - (begin + length));
     return new ByteArrayRef(begin, newData.length, newBack);
     }

     public ByteArrayRef append(byte[] newData) {
     int newLength = backingArray.length + newData.length;
     byte[] newBack = new byte[newLength];
     System.arraycopy(backingArray, 0, newBack, 0, begin + length);
     System.arraycopy(newData, 0, newBack, begin + length, newData.length);
     System.arraycopy(backingArray, begin + length, newBack, begin + length + newData.length, newLength - (begin + length));
     return new ByteArrayRef(begin, length + newData.length, newBack);
     }
    
     public ByteArrayRef delete() {
     int newLength = backingArray.length - this.length;
     if (newLength <= 4) {
     return new ByteArrayRef(0, 0, null); // completely deleted
     }
     byte[] newBack = new byte[newLength];
     System.arraycopy(backingArray, 0, newBack, 0, begin);
     System.arraycopy(backingArray, begin + length, newBack, begin, newLength - (begin + length));
     return new ByteArrayRef(begin, 0, newBack);
     }
   
     */
    /**
     * Copies the data referenced here into the target starting at offset
     *
     * @param target
     * @param offset
     */
    public void copyTo(byte[] target, int offset) {
        if (backingArray == null) {
            return; // ignore
        }
        System.arraycopy(backingArray, begin, target, offset, length);
    }

    @Override
    public int compareTo(ByteArrayRef that) {
        if (this.length != that.length) {
            return this.length - that.length;
        }
        for (int i = 0; i < this.length; i++) {
            byte thisB = this.backingArray[begin + i];
            byte thatB = that.backingArray[that.begin + i];
            if (thisB != thatB) {
                return thisB - thatB;
            }
        }
        return 0;
    }

    public int compareTo(byte[] that) {
        if (this.length != that.length) {
            return this.length - that.length;
        }
        for (int i = 0; i < this.length; i++) {
            byte thisB = this.backingArray[begin + i];
            byte thatB = that[i];
            if (thisB != thatB) {
                return thisB - thatB;
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof ByteArrayRef) {
            return this.compareTo((ByteArrayRef) that) == 0;
        } else if (that instanceof byte[]) {
            return this.compareTo((byte[]) that) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        if (this.backingArray == null) {
            return 97 * hash; // + 0
        }
        for (int i = this.begin; i < (this.begin + this.length); i++) {
            hash = 97 * hash + backingArray[i];
        }
        return hash;
    }

}
