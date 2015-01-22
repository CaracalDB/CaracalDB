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

import com.google.common.primitives.Ints;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import se.sics.caracaldb.global.SchemaData;

/**
 *
 * @author lkroll
 */
public class HashIdGenerator {
    
    private final MessageDigest md;
    
    public HashIdGenerator(String hashFunction) throws NoSuchAlgorithmException {
        md = MessageDigest.getInstance(hashFunction);
    }
    
    public byte[] idForName(String name) {
        byte[] nameB = name.getBytes(SchemaData.CHARSET);
        return md.digest(nameB);
    }
    
    public byte[] idForBytes(byte[] input) {
        return md.digest(input);
    }
    
    public byte[] idForNameDontStartWith(String name, byte[] reservedPrefix) {
        byte[] id = idForName(name);
        int i = 0;
        while (ByteArrayRef.rangeEquals(id, 0, reservedPrefix, 0, reservedPrefix.length)) {
            byte[] concatB = new byte[id.length+Ints.BYTES]; // avoid fixed point endless loop
            System.arraycopy(id, 0, concatB, 0, id.length);
            System.arraycopy(Ints.toByteArray(i), 0, concatB, id.length, Ints.BYTES);
            id = idForBytes(concatB);
            i++;
        }
        return id;
    }
   
}
