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
package se.sics.caracaldb.replication.linearisable;

import com.google.common.base.Optional;
import com.larskroll.common.BitBuffer;
import io.netty.buffer.ByteBuf;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.ServerSerializer;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.replication.linearisable.ExecutionEngine.SMROp;
import se.sics.caracaldb.replication.linearisable.ExecutionEngine.Scan;
import se.sics.caracaldb.replication.linearisable.ExecutionEngine.SyncedUp;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.UUIDSerializer;

/**
 *
 * @author lkroll
 */
public class XnginSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(XnginSerializer.class);

    private static final Boolean[] OP = new Boolean[]{false, false};
    private static final Boolean[] SYNCED = new Boolean[]{true, false};
    private static final Boolean[] SCAN = new Boolean[]{true, true};

    @Override
    public int identifier() {
        return ServerSerializer.XNGIN.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof SMROp) {
            SMROp op = (SMROp) o;
            byte[] flags = BitBuffer.create(OP).finalise();
            buf.writeBytes(flags);
            UUIDSerializer.INSTANCE.toBinary(op.id, buf);
            Serializers.toBinary(op.op, buf);
            return;
        }
        if (o instanceof SyncedUp) {
            SyncedUp op = (SyncedUp) o;
            byte[] flags = BitBuffer.create(SYNCED).finalise();
            buf.writeBytes(flags);
            UUIDSerializer.INSTANCE.toBinary(op.id, buf);
            return;
        }
        if (o instanceof Scan) {
            Scan op = (Scan) o;
            byte[] flags = BitBuffer.create(SCAN).finalise();
            buf.writeBytes(flags);
            UUIDSerializer.INSTANCE.toBinary(op.id, buf);
            CustomSerialisers.serialiseKeyRange(op.range, buf);
            return;
        }
        LOG.warn("Couldn't serialize {}: {}", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte[] flagsB = new byte[1];
        buf.readBytes(flagsB);
        boolean[] flags = BitBuffer.extract(8, flagsB);
        if (matches(flags, OP)) {
            UUID id = (UUID) UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            CaracalOp op = (CaracalOp) Serializers.fromBinary(buf, Optional.absent());
            return new SMROp(id, op);
        }
        if (matches(flags, SYNCED)) {
            UUID id = (UUID) UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            return new SyncedUp(id);
        }
        if (matches(flags, SCAN)) {
            UUID id = (UUID) UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            KeyRange range = CustomSerialisers.deserialiseKeyRange(buf);
            return new Scan(id, range);
        }
        return null;
    }

    private boolean matches(boolean[] flags, Boolean[] type) {
        return (flags[0] == type[0]) && (flags[1] == type[1]);
    }

}
