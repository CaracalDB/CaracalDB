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
package se.sics.caracaldb.utils;

import com.google.common.collect.ImmutableSortedSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.net.InetAddress;
import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.BitBuffer;

/**
 *
 * @author sario
 */
@RunWith(JUnit4.class)
public class CustomSerialisersTest {
    

    @Test
    public void bitBufferTest() {
        BitBuffer b1 = BitBuffer.create(true);
        boolean[] b1e = BitBuffer.extract(1, b1.finalise());
        assertEquals(true, b1e[0]);
        BitBuffer b2 = BitBuffer.create(false);
        boolean[] b2e = BitBuffer.extract(1, b2.finalise());
        assertEquals(false, b2e[0]);

        BitBuffer b3 = BitBuffer.create(true, false, true);
        boolean[] b3e = BitBuffer.extract(3, b3.finalise());
        assertEquals(true, b3e[0]);
        assertEquals(false, b3e[1]);
        assertEquals(true, b3e[2]);

        Boolean[] input = new Boolean[]{true, false, true, true, false, true,
            true, true, false, true, false, true, false, false, true, false, true, true};
        BitBuffer b4 = BitBuffer.create(input);
        boolean[] b4e = BitBuffer.extract(input.length, b4.finalise());
        for (int i = 0; i < input.length; i++) {
            assertEquals(input[i], b4e[i]);
        }
    }

    @Test
    public void keyTest() throws IOException {
        /*
         * Keys
         */
        Key nullKey = new Key((byte[]) null);
        Key someKey = Key.fromHex("FF EE 00 11 AA 01 12 34 FE");

        ByteBuf buf = Unpooled.buffer();

        CustomSerialisers.serialiseKey(Key.INF, buf);
        CustomSerialisers.serialiseKey(nullKey, buf);
        CustomSerialisers.serialiseKey(Key.NULL_KEY, buf);
        CustomSerialisers.serialiseKey(Key.ZERO_KEY, buf);
        CustomSerialisers.serialiseKey(someKey, buf);

        assertEquals(Key.INF, CustomSerialisers.deserialiseKey(buf));
        assertEquals(nullKey, CustomSerialisers.deserialiseKey(buf));
        assertEquals(Key.NULL_KEY, CustomSerialisers.deserialiseKey(buf));
        assertEquals(Key.ZERO_KEY, CustomSerialisers.deserialiseKey(buf));
        assertEquals(someKey, CustomSerialisers.deserialiseKey(buf));

        buf.release();

    }

    @Test
    public void viewTest() {

        try {
            InetAddress ip = InetAddress.getByAddress(new byte[]{1, 2, 3, 4});

            Address hostAddress = new Address(ip, 1234, null);
            Address someAddress = hostAddress.newVirtual(Key.fromHex("FF EE 00 11 AA 01 12 34 FE").getArray());
            Address someOtherAddress = hostAddress.newVirtual(Key.fromHex("FF EE 00 11 AA 01 12 34 FF").getArray());

            View v = new View(ImmutableSortedSet.of(someAddress, someOtherAddress), 1);

            ByteBuf buf = Unpooled.buffer();

            CustomSerialisers.serialiseView(v, buf);

            assertEquals(v, CustomSerialisers.deserialiseView(buf));

            buf.release();

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Test
    public void keyRangeTest() throws IOException {
        /*
         * Keys
         */
        Key someKey = Key.fromHex("FF EE 00 11 AA 01 12 34 FE");
        Key someOtherKey = Key.fromHex("FF FF AA 01 12 34 FE");
        KeyRange oo = KeyRange.open(someKey).open(someOtherKey);
        KeyRange oc = KeyRange.open(someKey).closed(someOtherKey);
        KeyRange co = KeyRange.closed(someKey).open(someOtherKey);
        KeyRange cc = KeyRange.closed(someKey).closed(someOtherKey);

        ByteBuf buf = Unpooled.buffer();

        CustomSerialisers.serialiseKeyRange(oo, buf);
        CustomSerialisers.serialiseKeyRange(oc, buf);
        CustomSerialisers.serialiseKeyRange(co, buf);
        CustomSerialisers.serialiseKeyRange(cc, buf);

        assertEquals(oo, CustomSerialisers.deserialiseKeyRange(buf));
        assertEquals(oc, CustomSerialisers.deserialiseKeyRange(buf));
        assertEquals(co, CustomSerialisers.deserialiseKeyRange(buf));
        assertEquals(cc, CustomSerialisers.deserialiseKeyRange(buf));

        buf.release();
    }
}
