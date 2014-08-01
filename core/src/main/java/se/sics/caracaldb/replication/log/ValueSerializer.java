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
package se.sics.caracaldb.replication.log;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;

/**
 *
 * @author lkroll
 */
public class ValueSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(ValueSerializer.class);

    static final byte NOOP = 0;
    static final byte RECONFIGURE = 1;

    @Override
    public int identifier() {
        return CoreSerializer.VALUE.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (!(o instanceof Value)) {
            LOG.error("Can't serialize {}:{}!", o, o.getClass());
            return;
        }
        Value val = (Value) o;
        SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(val.id, buf);
        if (val instanceof Noop) {
            buf.writeByte(NOOP);
            return;
        }
        if (val instanceof Reconfigure) {
            buf.writeByte(RECONFIGURE);
            Reconfigure r = (Reconfigure) val;
            CustomSerialisers.serialiseView(r.view, buf);
            buf.writeInt(r.quorum);
            buf.writeInt(r.versionId);
            CustomSerialisers.serialiseKeyRange(r.responsibility, buf);
            return;
        }
        LOG.error("Can't serialize {}:{}!", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        byte type = buf.readByte();
        switch (type) {
            case NOOP:
                return Noop.val;
            case RECONFIGURE:
                View v = CustomSerialisers.deserialiseView(buf);
                int quorum = buf.readInt();
                int versionId = buf.readInt();
                KeyRange r = CustomSerialisers.deserialiseKeyRange(buf);
                return new Reconfigure(id, v, quorum, versionId, r);
            default:
                LOG.error("Can't deserialize for type {}!", type);
                return null;
        }
    }

}
