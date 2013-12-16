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

import com.google.common.io.Closer;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.utils.CustomSerialisers.BitBuffer;
import se.sics.kompics.address.Address;

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
    public void keyTest() {
        /*
         * Keys
         */
        Key nullKey = new Key((byte[]) null);
        Key someKey = Key.fromHex("FF EE 00 11 AA 01 12 34 FE");

        byte[] data = null;
        try {
            /*
             * Write IN
             */
            Closer closer = Closer.create();
            try {
                ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
                DataOutputStream w = closer.register(new DataOutputStream(baos));

                CustomSerialisers.serialiseKey(Key.INF, w);
                CustomSerialisers.serialiseKey(nullKey, w);
                CustomSerialisers.serialiseKey(Key.NULL_KEY, w);
                CustomSerialisers.serialiseKey(Key.ZERO_KEY, w);
                CustomSerialisers.serialiseKey(someKey, w);

                data = baos.toByteArray();
            } catch (Throwable e) {
                throw closer.rethrow(e);
            } finally {
                closer.close();
            }

            assertNotNull(data);
            /*
             * Read OUT
             */
            closer = Closer.create();
            try {
                ByteArrayInputStream bais = closer.register(new ByteArrayInputStream(data));
                DataInputStream r = closer.register(new DataInputStream(bais));

                assertEquals(Key.INF, CustomSerialisers.deserialiseKey(r));
                assertEquals(nullKey, CustomSerialisers.deserialiseKey(r));
                assertEquals(Key.NULL_KEY, CustomSerialisers.deserialiseKey(r));
                assertEquals(Key.ZERO_KEY, CustomSerialisers.deserialiseKey(r));
                assertEquals(someKey, CustomSerialisers.deserialiseKey(r));

            } catch (Throwable e) {
                throw closer.rethrow(e);
            } finally {
                closer.close();
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }

    }

    @Test
    public void addressTest() {

        try {
            InetAddress ip = InetAddress.getByAddress(new byte[]{1, 2, 3, 4});
            /*
             * Addresses
             */

            Address hostAddress = new Address(ip, 1234, null);
            Address someAddress = hostAddress.newVirtual(Key.fromHex("FF EE 00 11 AA 01 12 34 FE").getArray());

            byte[] data = null;
            /*
             * Write IN
             */
            Closer closer = Closer.create();
            try {
                ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
                DataOutputStream w = closer.register(new DataOutputStream(baos));

                CustomSerialisers.serialiseAddress(hostAddress, w);
                CustomSerialisers.serialiseAddress(someAddress, w);

                data = baos.toByteArray();
            } catch (Throwable e) {
                throw closer.rethrow(e);
            } finally {
                closer.close();
            }

            assertNotNull(data);
            /*
             * Read OUT
             */
            closer = Closer.create();
            try {
                ByteArrayInputStream bais = closer.register(new ByteArrayInputStream(data));
                DataInputStream r = closer.register(new DataInputStream(bais));

                assertEquals(hostAddress, CustomSerialisers.deserialiseAddress(r));
                assertEquals(someAddress, CustomSerialisers.deserialiseAddress(r));

            } catch (Throwable e) {
                throw closer.rethrow(e);
            } finally {
                closer.close();
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Test
    public void keyRangeTest() {
        /*
         * Keys
         */
        Key someKey = Key.fromHex("FF EE 00 11 AA 01 12 34 FE");
        Key someOtherKey = Key.fromHex("FF FF AA 01 12 34 FE");
        KeyRange oo = KeyRange.open(someKey).open(someOtherKey);
        KeyRange oc = KeyRange.open(someKey).closed(someOtherKey);
        KeyRange co = KeyRange.closed(someKey).open(someOtherKey);
        KeyRange cc = KeyRange.closed(someKey).closed(someOtherKey);

        byte[] data = null;
        try {
            /*
             * Write IN
             */
            Closer closer = Closer.create();
            try {
                ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
                DataOutputStream w = closer.register(new DataOutputStream(baos));

                CustomSerialisers.serialiseKeyRange(oo, w);
                CustomSerialisers.serialiseKeyRange(oc, w);
                CustomSerialisers.serialiseKeyRange(co, w);
                CustomSerialisers.serialiseKeyRange(cc, w);

                data = baos.toByteArray();
            } catch (Throwable e) {
                throw closer.rethrow(e);
            } finally {
                closer.close();
            }

            assertNotNull(data);
            /*
             * Read OUT
             */
            closer = Closer.create();
            try {
                ByteArrayInputStream bais = closer.register(new ByteArrayInputStream(data));
                DataInputStream r = closer.register(new DataInputStream(bais));

                assertEquals(oo, CustomSerialisers.deserialiseKeyRange(r));
                assertEquals(oc, CustomSerialisers.deserialiseKeyRange(r));
                assertEquals(co, CustomSerialisers.deserialiseKeyRange(r));
                assertEquals(cc, CustomSerialisers.deserialiseKeyRange(r));

            } catch (Throwable e) {
                throw closer.rethrow(e);
            } finally {
                closer.close();
            }

        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
