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
package se.sics.caracaldb.system;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.CoreSerializer;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 *
 * @author lkroll
 */
public class SystemSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(SystemSerializer.class);

    static final boolean START = false;
    static final boolean STOP = true;

    @Override
    public int identifier() {
        return CoreSerializer.SYSTEM.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (!(o instanceof SystemMsg)) {
            LOG.error("Can't serialize {}:{}", o, o.getClass());
            return;
        }
        SystemMsg msg = (SystemMsg) o;
        if (msg instanceof StartVNode) {
            SpecialSerializers.MessageSerializationUtil.msgToBinary(msg, buf, START, false);
            StartVNode s = (StartVNode) msg;
            buf.writeInt(s.nodeId.length);
            buf.writeBytes(s.nodeId);
            return;
        }
        if (msg instanceof StopVNode) {
            SpecialSerializers.MessageSerializationUtil.msgToBinary(msg, buf, STOP, false);
            return;
        }
        LOG.error("Can't serialize {}:{}", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Class> hint) {
        MessageFields fields = SpecialSerializers.MessageSerializationUtil.msgFromBinary(buf);
        if (fields.flag1 == START) {
            int size = buf.readInt();
            byte[] nodeId = new byte[size];
            buf.readBytes(nodeId);
            return new StartVNode(fields.src, fields.dst, nodeId);
        }
        if (fields.flag1 == STOP) {
            return new StopVNode(fields.src, fields.dst);
        }
        LOG.error("Can't deserialize {}", fields);
        return null;
    }

}
