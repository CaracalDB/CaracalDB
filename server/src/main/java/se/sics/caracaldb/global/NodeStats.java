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

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;

/**
 *
 * @author sario
 */
public class NodeStats implements KompicsEvent {

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

    public void serialise(ByteBuf buf) throws IOException {
        SpecialSerializers.AddressSerializer.INSTANCE.toBinary(node, buf);
        CustomSerialisers.serialiseKeyRange(range, buf);
        buf.writeLong(storeSize);
        buf.writeLong(storeNumberOfKeys);
        buf.writeLong(ops);
    }

    public static NodeStats deserialise(ByteBuf buf) throws IOException {
        Address node = (Address) SpecialSerializers.AddressSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        KeyRange range = CustomSerialisers.deserialiseKeyRange(buf);
        long storeSize = buf.readLong();
        long storeNumberOfKeys = buf.readLong();
        long ops = buf.readLong();
        return new NodeStats(node, range, storeSize, storeNumberOfKeys, ops);
    }
}
