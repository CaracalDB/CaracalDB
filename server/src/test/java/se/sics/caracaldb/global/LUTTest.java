/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import com.google.common.primitives.Ints;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.caracaldb.Key;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
@RunWith(JUnit4.class)
public class LUTTest {

    private static final Random RAND = new Random(1);
    private static final int MAX_PORT = 2 ^ 16;

    @Test
    public void generationTest() {
        Set<Address> addrs = generateAddresses(5);
        LookupTable lut = LookupTable.generateInitial(addrs);

        assertEquals(addrs.size(), lut.numHosts());
        assertEquals(LookupTable.INIT_REP_FACTOR * addrs.size(), lut.numReplicationGroups());

        for (int i = 0; i < 5; i++) {
            assertNotNull("Should find responsible for any Key", lut.virtualHostsGetResponsible(randomKey(4)));
        }
    }

    @Test
    public void lutSerialisationtest() {
        try {
            final int n = 5;
            Set<Address> addrs = generateAddresses(n);
            LookupTable lut = LookupTable.generateInitial(addrs);

            byte[] lutbytes = lut.serialise();

            LookupTable lut2 = LookupTable.deserialise(lutbytes);

            assertEquals(addrs.size(), lut2.numHosts());
            assertEquals(LookupTable.INIT_REP_FACTOR * addrs.size(), lut2.numReplicationGroups());

            for (int i = 0; i < n; i++) {
                assertNotNull("Should find responsible for any Key", lut2.virtualHostsGetResponsible(randomKey(4)));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
    }

    @Test
    public void lookupGroupSerialisationtest() {
        try {
            LookupGroup lg = new LookupGroup((byte) 0);
            final int n = 100;
            HashSet<Key> keyset = new HashSet<Key>();
            for (int i = 0; i < n; i++) {
                Key k = randomKey(-1);
                keyset.add(k);
                lg.put(k, i);
            }
            byte[] lgbytes = lg.serialise();

            LookupGroup lg2 = LookupGroup.deserialise(lgbytes);

            for (Key k : keyset) {
                assertNotNull("Every key should be in group", lg.get(k));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
    }

    private Set<Address> generateAddresses(int n) {
        Set<Address> addresses = new HashSet<Address>(n);
        while (addresses.size() < n) {
            try {
                int port = RAND.nextInt(MAX_PORT);
                int ipInt = RAND.nextInt();
                InetAddress ip = InetAddress.getByAddress(Ints.toByteArray(ipInt));
                Address addr = new Address(ip, port, null);
                addresses.add(addr);
            } catch (UnknownHostException ex) {
                Logger.getLogger(LUTTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return addresses;
    }

    private Key randomKey(int size) {
        int s = size;
        if (size == -1) {
            s = Math.abs(RAND.nextInt(1000));
        }
        byte[] bytes = new byte[s];
        RAND.nextBytes(bytes);
        return new Key(bytes);
    }
}
