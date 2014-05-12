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
package se.sics.datamodel.util;

import com.google.common.primitives.Bytes;
import java.util.Arrays;
import java.util.Comparator;

import com.google.common.primitives.UnsignedBytes;
import java.util.Collection;
import se.sics.datamodel.DMSerializer;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
 //Immutable
public class ByteId implements Comparable<ByteId> {

    private static final Comparator<byte[]> lexiComp = UnsignedBytes.lexicographicalComparator();

    private final byte[] id;

    ByteId(Collection<Byte> id) {
        this.id = Bytes.toArray(id);
    }
    
    public ByteId(byte[] id) {
        this.id = Arrays.copyOf(id, id.length);
    }

    public byte[] getId() {
        return Arrays.copyOf(id, id.length);
    }

    @Override
    public String toString() {
        return DMSerializer.asString(id);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(id);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ByteId that = (ByteId) obj;

        return (compareTo(that) == 0);
    }

    @Override
    public int compareTo(ByteId that) {
        if (that == null) {
            throw new NullPointerException();
        }
        if (this.id.length == that.id.length) {
            return lexiComp.compare(this.id, that.id);
        }
        return (this.id.length < that.id.length) ? -1 : 1;
    }
}
