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
import com.google.common.primitives.Longs;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class TimestampIdFactory implements IdFactory {
    
    private static TimestampIdFactory instance;

    private final byte[] address;
    
    @Override
    public synchronized long newId() {
        byte[] millis = Longs.toByteArray(System.currentTimeMillis());
        byte[] nanos = Longs.toByteArray(System.nanoTime());
        return Longs.fromBytes(address[0], address[1], address[2], address[3], 
                millis[2], millis[1], millis[0], nanos[0]);
    }
    
    private TimestampIdFactory(Address adr) {
        this.address = Ints.toByteArray(adr.hashCode());
    }
    
    public static synchronized TimestampIdFactory get() {
        if (instance == null) {
            throw new RuntimeException("IdFactory has not yet been instatiated!");
        }
        return instance;
    }
    
    public static synchronized void init(Address adr) {
        instance = new TimestampIdFactory(adr);
    }
    
}
