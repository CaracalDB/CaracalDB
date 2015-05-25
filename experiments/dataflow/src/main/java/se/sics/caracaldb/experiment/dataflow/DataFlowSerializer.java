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
package se.sics.caracaldb.experiment.dataflow;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.util.UUID;
import se.sics.caracaldb.MessageSerializationUtil;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;

/**
 *
 * @author lkroll
 */
public class DataFlowSerializer implements Serializer {

    private static final boolean[] TS = new boolean[]{false, false};
    private static final boolean[] PING = new boolean[]{true, false};
    private static final boolean[] PONG = new boolean[]{true, true};

    @Override
    public int identifier() {
        return 150;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof TSMessage) {
            TSMessage msg = (TSMessage) o;
            MessageSerializationUtil.msgToBinary(msg, buf, TS[0], TS[1]);
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(msg.id, buf);
            buf.writeLong(msg.stats.bytes);
            buf.writeLong(msg.stats.time);
        }
        if (o instanceof Control.Ping) {
            Control.Ping msg = (Control.Ping) o;
            MessageSerializationUtil.msgToBinary(msg, buf, PING[0], PING[1]);
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(msg.id, buf);
            buf.writeLong(msg.ts);
        }
        if (o instanceof Control.Pong) {
            Control.Pong msg = (Control.Pong) o;
            MessageSerializationUtil.msgToBinary(msg, buf, PONG[0], PONG[1]);
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(msg.id, buf);
            buf.writeLong(msg.ts);
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        MessageSerializationUtil.MessageFields fields = MessageSerializationUtil.msgFromBinary(buf);
        UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        if (matches(fields, TS)) {
            long bytes = buf.readLong();
            long time = buf.readLong();
            return new TSMessage(fields.src, fields.dst, new TransferStats(bytes, time), id);
        }
        if (matches(fields, PING)) {
            long ts = buf.readLong();
            return new Control.Ping(fields.src, fields.dst, fields.proto, id, ts);
        }
        if (matches(fields, PONG)) {
            long ts = buf.readLong();
            return new Control.Pong(fields.src, fields.dst, fields.proto, id, ts);
        }
        return null;
    }

    private boolean matches(MessageSerializationUtil.MessageFields fields, boolean[] flags) {
        return fields.flag1 == flags[0] && fields.flag2 == flags[1];
    }

}
