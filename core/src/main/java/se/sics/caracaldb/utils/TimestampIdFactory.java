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
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.larskroll.math.FixedInteger;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class TimestampIdFactory implements IdFactory {

    private static TimestampIdFactory instance;
    private static final Logger LOG = LoggerFactory.getLogger(TimestampIdFactory.class);
    private static final int bytes = 10;
    // instance
    private final byte[] address;
    private FixedInteger ts;
    private com.larskroll.math.mutable.FixedInteger counter;
    private FixedInteger lastId;

    @Override
    public synchronized UUID newId() {
        checkUpdateTs();
        return nextId();
    }

    @Override
    public synchronized ImmutableSortedSet<UUID> newIds(int n) {
        checkUpdateTs();
        ImmutableSortedSet.Builder<UUID> idBuilder = ImmutableSortedSet.naturalOrder();
        for (int i = 0; i < n; i++) {
            idBuilder.add(nextId());
        }
        return idBuilder.build();
    }

    private UUID nextId() {
        byte[] lower = new byte[8];
        byte[] upper = new byte[8];
        lastId = ts.add(counter);
        counter.inc();
        byte[] idB = lastId.array();
        if (idB.length < 10) {
            LOG.error("Timestamp underflow! Should be 10 bytes but is only {}", idB.length);
            throw new RuntimeException("Fuck BigIntegers -.-");
        }
        if (idB.length > 10) {
            LOG.error("Timestamp overflow! It must be the end of days -.-");
            throw new RuntimeException("Out of timestamps! Try again in another universe...");
        }
        System.arraycopy(idB, 0, upper, 0, 8);
        System.arraycopy(idB, 8, lower, 0, 2);
        System.arraycopy(address, 0, lower, 2, 6);
        long up = Longs.fromByteArray(upper);
        long low = Longs.fromByteArray(lower);
        return new UUID(up, low);
    }

    private TimestampIdFactory(Address adr) {
        this.address = new byte[6];
        System.arraycopy(adr.getIp().getAddress(), 0, this.address, 0, 4);
        System.arraycopy(Ints.toByteArray(adr.getPort()), 2, this.address, 4, 2);
        byte[] tsB = new byte[bytes];
        Arrays.fill(tsB, (byte) 0);
        System.arraycopy(Longs.toByteArray(System.currentTimeMillis()), 0, tsB, 0, 8);
        this.ts = new FixedInteger(tsB);
        this.counter = com.larskroll.math.mutable.FixedInteger.zero(bytes);
        this.lastId = FixedInteger.zero(bytes);
    }

    private void checkUpdateTs() {
        byte[] tsB = new byte[bytes];
        Arrays.fill(tsB, (byte) 0);
        System.arraycopy(Longs.toByteArray(System.currentTimeMillis()), 0, tsB, 0, 8);
        FixedInteger nextTs = new FixedInteger(tsB);
        if (nextTs.compareTo(lastId) > 0) {
            ts = nextTs;
            counter = com.larskroll.math.mutable.FixedInteger.zero(bytes);
        }
    }

    public static TimestampIdFactory get() {
        if (instance == null) {
            throw new RuntimeException("IdFactory has not yet been instatiated!");
        }
        return instance;
    }

    public static synchronized void init(Address adr) {
        instance = new TimestampIdFactory(adr);
    }

}
