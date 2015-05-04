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
package se.sics.caracaldb.flow;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import se.sics.caracaldb.CoreSerializer;
import se.sics.kompics.network.netty.serialization.Serializer;

/**
 *
 * @author lkroll
 */
public class CollectorDescriptionSerializer implements Serializer {

    private static final byte BACC = 1;
    private static final byte FCC = 2;
    
    private final ConcurrentMap<Integer, CollectorDescriptor> baccCache = 
            new ConcurrentHashMap<Integer, CollectorDescriptor>();
    private final ConcurrentMap<Long, CollectorDescriptor> fccCache = 
            new ConcurrentHashMap<Long, CollectorDescriptor>();

    @Override
    public int identifier() {
        return CoreSerializer.CDS.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof ByteArrayChunkCollector.Descriptor) {
            ByteArrayChunkCollector.Descriptor d = (ByteArrayChunkCollector.Descriptor) o;
            buf.writeByte(BACC);
            buf.writeInt(d.bytes);
            return;
        }
        if (o instanceof FileChunkCollector.Descriptor) {
            FileChunkCollector.Descriptor d = (FileChunkCollector.Descriptor) o;
            buf.writeByte(FCC);
            buf.writeLong(d.bytes);
            return;
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte id = buf.readByte();
        if (id == BACC) {
            int bytes = buf.readInt();
            CollectorDescriptor d = baccCache.get(bytes);
            if (d == null) {
                d =  ByteArrayChunkCollector.descriptor(bytes);
                baccCache.put(bytes, d);
            }
            return d;
        }
        if (id == FCC) {
            long bytes = buf.readLong();
            CollectorDescriptor d = fccCache.get(bytes);
            if (d == null) {
                d =  FileChunkCollector.descriptor(bytes);
                fccCache.put(bytes, d);
            }
            return d;
        }
        return null;
    }

}
