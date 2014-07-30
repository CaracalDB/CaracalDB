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
package se.sics.caracaldb.persistence;

import com.google.common.primitives.Ints;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.caracaldb.utils.ByteArrayRef;

/**
 *
 * @author lkroll
 */
public abstract class MultiVersionUtil {

    private static final byte[] MANY_ONES = Ints.toByteArray(-1);
    private static final int LOWER2BYTES = 0x0000FFFF;

    public static final int MAX_DATA_SIZE = 65534; // 2^16 (2 bytes) - 2 ("-1" is flag for no next value)
    public static final int MAX_VERSION = 65535; // again 2 bytes no reserved values

    public static byte[] pack(SortedMap<Integer, ByteArrayRef> values) {
        if (values.isEmpty()) {
            return null; // this is pretty much a total key delete
        }
        // first figure out the total array length
        int length = 0;
        for (Entry<Integer, ByteArrayRef> e : values.entrySet()) {
            length += 4; // header size;
            length += e.getValue().length; // data
            if (e.getValue().length > MAX_DATA_SIZE) {
                return null; // this is pretty bad...should have been caught at the client already
            }
        }
        // then write all the data correctly into the array
        byte[] data = new byte[length];
        int ptr = 0;
        int lastHeader = 0;
        for (Entry<Integer, ByteArrayRef> e : values.entrySet()) {
            packHeader(e.getKey(), e.getValue().length, data, ptr);
            lastHeader = ptr;
            ptr += 4;
            e.getValue().copyTo(data, ptr);
            ptr += e.getValue().length;
        }
        // rewrite last header to -1 instead of data length (since data length is clearly just until the end of array)
        data[lastHeader + 2] = MANY_ONES[2];
        data[lastHeader + 3] = MANY_ONES[3];

        return data;
    }

    public static SortedMap<Integer, ByteArrayRef> unpack(byte[] data) {
        TreeMap<Integer, ByteArrayRef> values = new TreeMap<Integer, ByteArrayRef>();
        if (data == null) {
            return values;
        }
        Pair<Integer, Integer> header = unpackHeader(data, 0);
        int version = header.getValue0();
        int length = header.getValue1();
        int ptr = 4;
        while (!isEnd(length)) {
            values.put(version, new ByteArrayRef(ptr, length, data));
            ptr += length;
            header = unpackHeader(data, ptr);
            version = header.getValue0();
            length = header.getValue1();
            ptr += 4;
        }
        values.put(version, new ByteArrayRef(ptr, data.length - ptr, data));
        return values;
    }

    private static void packHeader(int version, int length, byte[] blob, int offset) {
        byte[] versionB = Ints.toByteArray(version);
        byte[] lengthB = Ints.toByteArray(length);
        blob[offset] = versionB[2];
        blob[offset + 1] = versionB[3];
        blob[offset + 2] = lengthB[2];
        blob[offset + 3] = lengthB[3];
    }

    private static Pair<Integer, Integer> unpackHeader(byte[] blob, int offset) {
        int version = Ints.fromBytes((byte) 0, (byte) 0, blob[offset], blob[offset + 1]);
        int length = Ints.fromBytes((byte) 0, (byte) 0, blob[offset + 2], blob[offset + 3]);
        return Pair.with(version, length);
    }

    private static boolean isEnd(int length) {
        return length == LOWER2BYTES;
    }
}
