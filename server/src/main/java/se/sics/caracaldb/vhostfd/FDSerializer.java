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

package se.sics.caracaldb.vhostfd;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.ServerSerializer;
import se.sics.caracaldb.vhostfd.VirtualEPFD.Ping;
import se.sics.caracaldb.vhostfd.VirtualEPFD.Pong;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 *
 * @author lkroll
 */
public class FDSerializer implements Serializer {
    
    private static final Logger LOG = LoggerFactory.getLogger(FDSerializer.class);
    
    private static final boolean PING = false;
    private static final boolean PONG = true;

    @Override
    public int identifier() {
        return ServerSerializer.FD.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof Ping) {
            Ping p = (Ping) o;
            SpecialSerializers.MessageSerializationUtil.msgToBinary(p, buf, PING, false);
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(p.id, buf);
            buf.writeLong(p.ts);
            return;
        }
        if (o instanceof Pong) {
            Pong p = (Pong) o;
            SpecialSerializers.MessageSerializationUtil.msgToBinary(p, buf, PONG, false);
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(p.id, buf);
            buf.writeLong(p.ts);
            return;
        }
        LOG.warn("Couldn't serialise instance {} of {}", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Class> hint) {
        MessageFields fields = SpecialSerializers.MessageSerializationUtil.msgFromBinary(buf);
        UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        long ts = buf.readLong();
        if (fields.flag1 == PING) {
            return new Ping(id, ts, fields.src, fields.dst);
        }
        if (fields.flag1 == PONG) {
            return new Pong(id, ts, fields.src, fields.dst);
        }
        return null; // Shouldn't get here
    }
    
}
