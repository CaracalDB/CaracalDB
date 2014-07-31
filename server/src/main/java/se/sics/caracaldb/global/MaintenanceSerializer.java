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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.ServerSerializer;
import se.sics.caracaldb.View;
import se.sics.caracaldb.replication.linearisable.ViewChange;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.BitBuffer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 *
 * @author lkroll
 */
public class MaintenanceSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(MaintenanceSerializer.class);

    // 0
    private static final boolean MSG = false;
    private static final boolean OP = true;
    // 1&2
    private static final Boolean[] JOIN = new Boolean[]{false, false};
    private static final Boolean[] SYNCED = new Boolean[]{false, true};
    private static final Boolean[] RECONF = new Boolean[]{true, false};
    private static final Boolean[] UPDATE = new Boolean[]{true, true};

    @Override
    public int identifier() {
        return ServerSerializer.GLOBAL.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof MaintenanceMsg) {
            MaintenanceMsg msg = (MaintenanceMsg) o;
            int flagPos = buf.writerIndex();
            buf.writeByte(0); // reserve for flags
            BitBuffer flags = BitBuffer.create(MSG); // 0
            SpecialSerializers.MessageSerializationUtil.msgToBinary(msg, buf, false, false);
            toBinaryOp(msg.op, buf, flags);
            byte[] flagsB = flags.finalise();
            buf.setByte(flagPos, flagsB[0]);
            return;
        }
        if (o instanceof Maintenance) {
            int flagPos = buf.writerIndex();
            buf.writeByte(0); // reserve for flags
            BitBuffer flags = BitBuffer.create(OP); // 0
            toBinaryOp((Maintenance) o, buf, flags);
            byte[] flagsB = flags.finalise();
            buf.setByte(flagPos, flagsB[0]);
            return;
        }
        LOG.warn("Couldn't serialize {}: {}", o, o.getClass());
    }

    private void toBinaryOp(Maintenance op, ByteBuf buf, BitBuffer flags) {
        if (op instanceof NodeJoin) {
            NodeJoin m = (NodeJoin) op;
            flags.write(JOIN);
            flags.write(false, false); // reserve bits 3&4
            CustomSerialisers.serialiseView(m.view, buf);
            CustomSerialisers.serialiseKeyRange(m.responsibility, buf);
            flags.write(m.dataTransfer); // custom bit 5
            buf.writeInt(m.quorum);
            return;
        }
        if (op instanceof NodeSynced) {
            NodeSynced m = (NodeSynced) op;
            flags.write(SYNCED);
            flags.write(false, false); // reserve bits 3&4
            CustomSerialisers.serialiseView(m.view, buf);
            CustomSerialisers.serialiseKeyRange(m.responsibility, buf);
            return;
        }
        if (op instanceof Reconfiguration) {
            Reconfiguration m = (Reconfiguration) op;
            flags.write(RECONF);
            flags.write(false, false); // reserve bits 3&4
            CustomSerialisers.serialiseView(m.change.view, buf);
            buf.writeInt(m.change.quorum);
            CustomSerialisers.serialiseKeyRange(m.change.range, buf);
            return;
        }
        if (op instanceof LUTUpdate) {
            LUTUpdate m = (LUTUpdate) op;
            flags.write(UPDATE);
            flags.write(false, false); // reserve bits 3&4
            m.serialise(buf);
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Class> hint) {
                byte[] flagsB = new byte[1];
        buf.readBytes(flagsB);
        boolean[] flags = BitBuffer.extract(8, flagsB);
        if (flags[0] == MSG) {
            MessageFields fields = SpecialSerializers.MessageSerializationUtil.msgFromBinary(buf);
            Maintenance op = fromBinaryOp(buf, flags);
            return new MaintenanceMsg(fields.src, fields.dst, op);
        }
        if (flags[0] == OP) {
            return fromBinaryOp(buf, flags);
        }
        return null; // no idea how it should get here^^
    }

    private Maintenance fromBinaryOp(ByteBuf buf, boolean[] flags) {
        if (matches(flags, JOIN)) {
            View view = CustomSerialisers.deserialiseView(buf);
            KeyRange respon = CustomSerialisers.deserialiseKeyRange(buf);
            boolean dataTransfer = flags[5];
            int quorum = buf.readInt();
            return new NodeJoin(view, quorum, respon, dataTransfer);
        }
        if (matches(flags, SYNCED)) {
            View view = CustomSerialisers.deserialiseView(buf);
            KeyRange respon = CustomSerialisers.deserialiseKeyRange(buf);
            return new NodeSynced(view, respon);
        }
        if (matches(flags, RECONF)) {
            View view = CustomSerialisers.deserialiseView(buf);
            int quorum = buf.readInt();
            KeyRange range = CustomSerialisers.deserialiseKeyRange(buf);
            return new Reconfiguration(new ViewChange(view, quorum, range));
        }
        if (matches(flags, UPDATE)) {
            try {
                return LUTUpdate.deserialise(buf);
            } catch (InstantiationException ex) {
                LOG.error("Could not deserialise LUTUpdate: \n   ", ex);
            } catch (IllegalAccessException ex) {
                LOG.error("Could not deserialise LUTUpdate: \n   ", ex);
            }
        }
        return null;
    }
    
    private boolean matches(boolean[] flags, Boolean[] type) {
        return (flags[1] == type[0]) && (flags[2] == type[1]);
    }

}
