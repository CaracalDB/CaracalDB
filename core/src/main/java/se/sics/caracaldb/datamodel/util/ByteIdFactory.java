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
package se.sics.caracaldb.datamodel.util;

import java.util.Arrays;
import java.util.LinkedList;
import se.sics.kompics.address.IdUtils;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ByteIdFactory {

    public static final int MAX_BYTE_SIZE = 255;
    public static final ByteId MIN_BYTE_ID;
    public static final ByteId MAX_BYTE_ID;

    static {
        MIN_BYTE_ID = new ByteId(new byte[]{1, 0});

        byte[] maxByteId = new byte[256];
        Arrays.fill(maxByteId, (byte)255);
        MAX_BYTE_ID = new ByteId(maxByteId);
    }

    private LinkedList<Byte> id;
    private boolean maxIdReached;

    /**
     * returns sequential ids starting from 1
     */
    public ByteIdFactory() {
        this.id = new LinkedList<Byte>();
        this.id.add((byte) 1);
        this.id.add((byte) 0);
        this.maxIdReached = false;
    }

    /**
     * @return null if maxId is reached
     */
    public ByteId nextId() {
        if (maxIdReached) {
            return null;
        }
        id = ByteIdFactory.incrementId(id);
        if (id == null) {
            maxIdReached = true;
            return null;
        }
        return new ByteId(id);
    }

    public static ByteId nextId(ByteId id) {
        LinkedList<Byte> idList = new LinkedList<Byte>();
        for (Byte b : id.getId()) {
            idList.add(b);
        }
        idList = incrementId(idList);
        if (idList == null) {
            return null;
        }
        return new ByteId(idList);
    }

    private static LinkedList<Byte> incrementId(LinkedList<Byte> idList) {
        boolean carry = true;
        for (int index = idList.size() - 1; index > 0; index--) {
            Byte b = idList.remove(index);
            if (b.equals((byte) 255)) {
                b = (byte) 0;
                carry = true;
            } else {
                b++;
                carry = false;
            }
            idList.add(index, b);
            if (!carry) {
                break;
            }
        }
        if (carry) {
            Byte b = idList.removeFirst();
            if (b.equals(MAX_BYTE_SIZE)) {
                return null;
            }
            b++;
            idList.addFirst((byte) 1);
            idList.addFirst(b);
            return idList;
        } else {
            return idList;
        }
    }
}
