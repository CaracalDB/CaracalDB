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
package se.sics.caracaldb.global;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;

/**
 *
 * @author sario
 */
public class NodeStats extends Event {

    public final Address node;
    public final KeyRange range;
    public final long storeSize;
    public final long storeNumberOfKeys;
    public final long ops;

    public NodeStats(Address node, KeyRange range, long storeSize, long storeNumberOfKeys, long ops) {
        this.node = node;
        this.range = range;
        this.storeSize = storeSize;
        this.storeNumberOfKeys = storeNumberOfKeys;
        this.ops = ops;
    }
    
    public void serialise(DataOutputStream w) throws IOException {
        CustomSerialisers.serialiseAddress(node, w);
        CustomSerialisers.serialiseKeyRange(range, w);
        w.writeLong(storeSize);
        w.writeLong(storeNumberOfKeys);
        w.writeLong(ops);
    }
    
    public static NodeStats deserialise(DataInputStream r) throws IOException {
        Address node = CustomSerialisers.deserialiseAddress(r);
        KeyRange range = CustomSerialisers.deserialiseKeyRange(r);
        long storeSize = r.readLong();
        long storeNumberOfKeys = r.readLong();
        long ops = r.readLong();
        return new NodeStats(node, range, storeSize, storeNumberOfKeys, ops);
    }
}
