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
package se.sics.caracaldb.operations;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
//import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.store.MultiOp.Condition;
import se.sics.caracaldb.store.MultiOp.EqualCondition;
//import se.sics.caracaldb.store.MultiOp.FunctionalCondition;
import se.sics.caracaldb.utils.ByteArrayRef;
import se.sics.caracaldb.utils.CustomSerialisers;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.BitBuffer;

/**
 *
 * @author lkroll
 */
public class ConditionSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(ConditionSerializer.class);

    public final Boolean[] FUNC = new Boolean[]{false};
    public final Boolean[] EQ = new Boolean[]{true};

    @Override
    public int identifier() {
        return CoreSerializer.COND.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof Condition) {
            Condition c = (Condition) o;
            int flagPos = buf.writerIndex();
            buf.writeByte(0); // reserve for flags
            BitBuffer flags = BitBuffer.create(false); // reserve 0
            toBinaryCondition(c, buf, flags);
            byte[] flagsB = flags.finalise();
            buf.setByte(flagPos, flagsB[0]);
            return;
        }
        LOG.warn("Couldn't serialize {}: {}", o, o.getClass());
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Object> hint) {
        byte[] flagsB = new byte[1];
        buf.readBytes(flagsB);
        boolean[] flags = BitBuffer.extract(8, flagsB);
        return fromBinaryCondition(buf, flags);
    }

    private void toBinaryCondition(Condition c, ByteBuf buf, BitBuffer flags) {
        // if (c instanceof FunctionalCondition) {
        //     flags.write(FUNC); // 1
        //     FunctionalCondition fc = (FunctionalCondition) c;
        //     CustomSerialisers.serialiseKey(fc.k, buf);
        //     Serializers.toBinary(fc.pred, buf);
        //     return;
        // }
        if (c instanceof EqualCondition) {
            flags.write(EQ); // 1
            EqualCondition ec = (EqualCondition) c;
            CustomSerialisers.serialiseKey(ec.k, buf);
            flags.write(false, false); // reserve 2 3
            if (ec.oldValue == null) {
                flags.write(false); // 4
            } else {
                flags.write(true); // 4
                buf.writeInt(ec.oldValue.length);
                buf.writeBytes(ec.oldValue);
            }
            return;
        }
    }

    private Condition fromBinaryCondition(ByteBuf buf, boolean[] flags) {
        if (matches(flags, FUNC)) {
            Key k = CustomSerialisers.deserialiseKey(buf);
            Serializers.fromBinary(buf, Optional.absent());
            // Predicate<ByteArrayRef> pred = (Predicate<ByteArrayRef>) Serializers.fromBinary(buf, Optional.absent());
            // return new FunctionalCondition(k, pred);
            return null;
        }
        if (matches(flags, EQ)) {
            Key k = CustomSerialisers.deserialiseKey(buf);
            if (flags[4]) {
                int l = buf.readInt();
                byte[] v = new byte[l];
                buf.readBytes(v);
                return new EqualCondition(k, v);
            } else {
                return new EqualCondition(k, null);
            }
        }
        return null;
    }

    private boolean matches(boolean[] flags, Boolean[] type) {
        return flags[1] == type[0];
    }

}
