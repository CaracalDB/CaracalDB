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
package se.sics.caracaldb.bootstrap;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.ServerSerializer;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 *
 * @author lkroll
 */
public class BootstrapSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(BootstrapSerializer.class);

    // 0&1
    private static final Boolean[] REQ = new Boolean[]{false, false};
    //private static final Boolean[] RESP = new Boolean[]{false, true}; // free now
    private static final Boolean[] READY = new Boolean[]{true, false};
    private static final Boolean[] BOOT = new Boolean[]{true, true};

    @Override
    public int identifier() {
        return ServerSerializer.BOOT.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (!(o instanceof BootstrapMsg)) {
            LOG.warn("Couldn't serialize {}: {}", o, o.getClass());
            return;
        }
        if (o instanceof BootstrapRequest) {
            BootstrapRequest br = (BootstrapRequest) o;
            SpecialSerializers.MessageSerializationUtil.msgToBinary(br, buf, REQ[0], REQ[1]);
            return;
        }
        if (o instanceof Ready) {
            Ready r = (Ready) o;
            SpecialSerializers.MessageSerializationUtil.msgToBinary(r, buf, READY[0], READY[1]);
            return;
        }
        if (o instanceof BootUp) {
            BootUp bu = (BootUp) o;
            SpecialSerializers.MessageSerializationUtil.msgToBinary(bu, buf, BOOT[0], BOOT[1]);
            return;
        }
        LOG.warn("Couldn't serialize BootstrapMsg {}: {}", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        MessageFields fields = SpecialSerializers.MessageSerializationUtil.msgFromBinary(buf);
        if (matches(fields, REQ)) {
            return new BootstrapRequest(fields.orig, fields.src, fields.dst);
        }
        if (matches(fields, READY)) {
            return new Ready(fields.src, fields.dst);
        }
        if (matches(fields, BOOT)) {
            return new BootUp(fields.src, fields.dst);
        }
        return null; // no idea how it should get here
    }

    private boolean matches(MessageFields fields, Boolean[] flags) {
        return (fields.flag1 == flags[0]) && (fields.flag2 == flags[1]);
    }

}
