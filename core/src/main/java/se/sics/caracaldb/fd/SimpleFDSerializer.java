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
package se.sics.caracaldb.fd;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.MessageSerializationUtil;
import se.sics.caracaldb.fd.SimpleEFD.Heartbeat;
import se.sics.kompics.network.netty.serialization.Serializer;

/**
 *
 * @author lkroll
 */
public class SimpleFDSerializer implements Serializer {

    @Override
    public int identifier() {
        return CoreSerializer.SFD.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof Heartbeat) {
            Heartbeat hb = (Heartbeat) o;
            MessageSerializationUtil.msgToBinary(hb, buf, true, false);
            return;
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        MessageSerializationUtil.MessageFields fields = MessageSerializationUtil.msgFromBinary(buf);
        return new Heartbeat(fields.src, fields.dst);
    }

}
