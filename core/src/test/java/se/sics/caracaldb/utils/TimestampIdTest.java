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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.stream.IntStream;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.kompics.address.Address;

/**
 *
 * @author lkroll
 */
@RunWith(JUnit4.class)
public class TimestampIdTest {

    private static final int NUM = 10;

    @Before
    public void setUp() throws UnknownHostException {
        InetAddress ip = InetAddress.getByAddress(new byte[]{1, 2, 3, 4});
        Address hostAddress = new Address(ip, 1234, null);
        TimestampIdFactory.init(hostAddress);
    }

    @Test
    public void singleDistinctIdIdTest() {
        long num = IntStream.range(0, NUM).parallel().mapToObj((i) -> TimestampIdFactory.get().newId()).distinct().count();
        assertEquals(NUM, num);
    }

    @Test
    public void singleMonotonicTest() {
        UUID lastId = TimestampIdFactory.get().newId();
        IntStream.range(0, NUM).forEach((i) -> {
            UUID newID = TimestampIdFactory.get().newId();
            assertTrue(lastId.compareTo(newID) < 0);
        });
    }
    
    @Test
    public void listDistinctIdIdTest() {
        long num = TimestampIdFactory.get().newIds(NUM).stream().distinct().count();
        assertEquals(NUM, num);
    }

    @Test
    public void listMonotonicTest() {
        UUID lastId = TimestampIdFactory.get().newId();
        TimestampIdFactory.get().newIds(NUM).stream().forEach((i) -> {
            UUID newID = TimestampIdFactory.get().newId();
            assertTrue(lastId.compareTo(newID) < 0);
        });
    }
}
