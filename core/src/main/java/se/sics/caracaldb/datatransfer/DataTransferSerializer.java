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
package se.sics.caracaldb.datatransfer;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import java.nio.charset.Charset;
import java.util.Map.Entry;
import java.util.UUID;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.MessageSerializationUtil;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;

/**
 *
 * @author lkroll
 */
public class DataTransferSerializer implements Serializer {

    private static final boolean INIT = false;
    private static final boolean ACK_OR_AR = true;
    private static final boolean ACK = false;
    private static final boolean AR = true;

    private static final Charset UTF8 = Charset.forName("UTF8");

    @Override
    public int identifier() {
        return CoreSerializer.DTS.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof InitiateTransfer) {
            initToBinary((InitiateTransfer) o, buf);
            return;
        }
        if (o instanceof DataTransferComponent.Ack) {
            ackToBinary((DataTransferComponent.Ack) o, buf);
            return;
        }
        if (o instanceof DataTransferComponent.AllReceived) {
            arToBinary((DataTransferComponent.AllReceived) o, buf);
            return;
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        MessageSerializationUtil.MessageFields fields = MessageSerializationUtil.msgFromBinary(buf);
        if (fields.flag1 == INIT) {
            return initFromBinary(fields, buf);
        }
        if ((fields.flag1 == ACK_OR_AR) && (fields.flag2 == ACK)) {
            return ackFromBinary(fields, buf);
        }
        if ((fields.flag1 == ACK_OR_AR) && (fields.flag2 == AR)) {
            return arFromBinary(fields, buf);
        }
        return null;
    }

    private void initToBinary(InitiateTransfer init, ByteBuf buf) {
        MessageSerializationUtil.msgToBinary(init, buf, INIT, false);
        SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(init.id, buf);
        buf.writeInt(init.metadata.size());
        if (!init.metadata.isEmpty()) {
            for (Entry<String, Object> e : init.metadata.entrySet()) {
                byte[] keyB = e.getKey().getBytes(UTF8);
                buf.writeInt(keyB.length);
                buf.writeBytes(keyB);
                Serializers.toBinary(e.getValue(), buf);
            }
        }
    }

    private void ackToBinary(DataTransferComponent.Ack ack, ByteBuf buf) {
        MessageSerializationUtil.msgToBinary(ack, buf, ACK_OR_AR, ACK);
        SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(ack.id, buf);
    }
    
    private void arToBinary(DataTransferComponent.AllReceived ar, ByteBuf buf) {
        MessageSerializationUtil.msgToBinary(ar, buf, ACK_OR_AR, AR);
        SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(ar.id, buf);
    }

    private InitiateTransfer initFromBinary(MessageSerializationUtil.MessageFields fields, ByteBuf buf) {
        UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        int size = buf.readInt();
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        for (int i = 0; i < size; i++) {
            int keySize = buf.readInt();
            byte[] keyB = new byte[keySize];
            buf.readBytes(keyB);
            String key = new String(keyB, UTF8);
            Object value = Serializers.fromBinary(buf, Optional.absent());
            builder.put(key, value);
        }
        return new InitiateTransfer(fields.src, fields.dst, id, builder.build());
    }

    private DataTransferComponent.Ack ackFromBinary(MessageSerializationUtil.MessageFields fields, ByteBuf buf) {
        UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        return new DataTransferComponent.Ack(fields.src, fields.dst, id);
    }

    private DataTransferComponent.AllReceived arFromBinary(MessageSerializationUtil.MessageFields fields, ByteBuf buf) {
        UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
        return new DataTransferComponent.AllReceived(fields.src, fields.dst, id);
    }

}
