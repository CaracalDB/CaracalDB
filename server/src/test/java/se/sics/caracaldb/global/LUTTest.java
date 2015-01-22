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
package se.sics.caracaldb.global;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.Assert;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.Launcher;
import se.sics.caracaldb.utils.HashIdGenerator;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
@RunWith(JUnit4.class)
public class LUTTest {

    private static final Random RAND = new Random(1);
    private static final int MAX_PORT = 2 ^ 16;

    @Before
    public void setUp() {
        Launcher.reset();
    }

    @Test
    public void generationTest() {
        TreeSet<Address> addrs = generateAddresses(6);
        Configuration conf = Launcher.config().setValue("caracal.scatterWidth", 3).finalise();
        LookupTable lut = LookupTable.generateInitial(addrs, conf, addrs.first());

        System.out.println("%%%%%%% Generated LUT: %%%%%%%%\n");
        StringBuilder sb = new StringBuilder();
        lut.printFormat(sb);
        System.out.println(sb);

        assertEquals(addrs.size(), lut.numHosts());
        //assertEquals(LookupTable.INIT_REP_FACTOR * addrs.size(), lut.numReplicationSets());

        for (int i = 0; i < 6; i++) {
            try {
                assertNotNull("Should find responsible for any Key", lut.virtualHostsGetResponsible(randomKey(4, lut.schemas().schemaNames.keySet())));
            } catch (LookupTable.NoSuchSchemaException ex) {
                Assert.fail(ex.getMessage());
            }
        }

    }

    @Test
    public void bigGenerationTest() {
        TreeSet<Address> addrs = generateAddresses(500);
        Configuration conf = Launcher.config().setValue("caracal.scatterWidth", 20).finalise();
        LookupTable lut = LookupTable.generateInitial(addrs, conf, addrs.first());

        assertEquals(addrs.size(), lut.numHosts());
        //assertEquals(LookupTable.INIT_REP_FACTOR * addrs.size(), lut.numReplicationSets());

        for (int i = 0; i < 600; i++) {
            try {
                assertNotNull("Should find responsible for any Key", lut.virtualHostsGetResponsible(randomKey(4, lut.schemas().schemaNames.keySet())));
            } catch (LookupTable.NoSuchSchemaException ex) {
                Assert.fail(ex.getMessage());
            }
        }

        System.out.println("%%%%%%% Generated LUT: %%%%%%%%\n");
        StringBuilder sb = new StringBuilder();
        lut.printFormat(sb);
        System.out.println(sb);
    }

    @Test
    public void localNodesTest() {
        TreeSet<Address> addrs = generateAddresses(3);
        Configuration conf = Launcher.config().finalise();
        LookupTable lut = LookupTable.generateInitial(addrs, conf, addrs.first());

        StringBuilder sb = new StringBuilder();
        lut.printFormat(sb);
        System.out.println(sb.toString());
        ImmutableSortedSet<Key> expected = ImmutableSortedSet.of(Key.fromHex("09 8F 6B CD 46 21 D3 73 CA DE 4E 83 26 27 B4 F6"),
                Key.fromHex("09 8F 6B CD 46 21 D3 73 CA DE 4E 83 26 27 B4 F6 3F FF FF FF"),
                Key.fromHex("09 8F 6B CD 46 21 D3 73 CA DE 4E 83 26 27 B4 F6 7F FF FF FE"),
                Key.fromHex("09 8F 6B CD 46 21 D3 73 CA DE 4E 83 26 27 B4 F6 BF FF FF FD"),
                Key.fromHex("09 8F 6B CD 46 21 D3 73 CA DE 4E 83 26 27 B4 F6 FF FF FF FC"));

        for (Address adr : addrs) {
            Set<Key> localNodes = lut.getVirtualNodesAt(adr);
            localNodes.remove(Key.fromHex("00 00 00 00 00 00 00 01"));
            localNodes.remove(Key.fromHex("00 00 00 00 00 00 00 02"));
            System.out.println("Nodes for " + adr + " are " + localNodes);
            System.out.println("Expected: " + expected);
            assertTrue(Sets.symmetricDifference(localNodes, expected).isEmpty());
            for (Key nodeId : localNodes) {
                try {
                    Key succ = expected.higher(nodeId);
                    if (succ == null) {
                        succ = expected.first().inc();
                    }
                    KeyRange resp = lut.getResponsibility(nodeId);
                    KeyRange expResp = KeyRange.closed(nodeId).open(succ);
                    assertEquals(expResp, resp);
                } catch (LookupTable.NoSuchSchemaException ex) {
                    Assert.fail(ex.getMessage());
                }
            }
        }
    }

    @Test
    public void lutSerialisationTest() {
        try {
            final int n = 5;
            TreeSet<Address> addrs = generateAddresses(n);
            Configuration conf = Launcher.config().finalise();
            LookupTable lut = LookupTable.generateInitial(addrs, conf, addrs.first());

            byte[] lutbytes = lut.serialise();

            LookupTable lut2 = LookupTable.deserialise(lutbytes);

            assertEquals(addrs.size(), lut2.numHosts());
            //assertEquals(LookupTable.INIT_REP_FACTOR * addrs.size(), lut2.numReplicationSets());

            for (int i = 0; i < n; i++) {
                assertNotNull("Should find responsible for any Key", lut2.virtualHostsGetResponsible(randomKey(4, lut.schemas().schemaNames.keySet())));
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            fail(ex.getMessage());
        }
    }

    @Test
    public void lookupGroupSerialisationTest() {
        try {
            LookupGroup lg = new LookupGroup((byte) 0);
            final int n = 100;
            HashSet<Key> keyset = new HashSet<Key>();
            for (int i = 0; i < n; i++) {
                Key k = randomKeySuffix(-1);
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

    @Test
    public void keyTest() {
        Key k = new Key(new byte[]{0, 1, 2, 3});
        assertEquals("00 01 02 03", k.toString());

        k = new Key(1);
        assertEquals("00 00 00 01", k.toString());

        k = new Key("AB");
        assertEquals("41 42", k.toString());

        k = Key.fromHex("FF FF FF FD");
        assertEquals("FF FF FF FD", k.toString());
    }

    @Test
    public void schemaReaderTest() {
        SchemaData sd = new SchemaData();
        System.out.println("Putting...");
        ByteBuffer core = ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, 0});
        sd.schemaIDs.put("core", core);
        sd.schemaNames.put(core, "core");
        sd.metaData.put(core, ImmutableMap.of("testvalue1", "1", "testvalue2", "2"));

        ByteBuffer extent = ByteBuffer.wrap(new byte[]{0, 0, 0, 0, 0, 0, 0, 1});
        sd.schemaIDs.put("extent", extent);
        sd.schemaNames.put(extent, "extent");
        sd.metaData.put(extent, ImmutableMap.of("testvalue3", "3", "testvalue4", "4"));
        System.out.println("Exporting...");

        Assert.assertFalse(core.equals(extent));

        String json = "";
        try {
            json = SchemaReader.exportSchemas(sd, null);
        } catch (FileNotFoundException ex) {
            Assert.fail(ex.getMessage());
        } catch (UnsupportedEncodingException ex) {
            Assert.fail(ex.getMessage());
        }

        System.out.println("Importing...");

        SchemaData sd2 = null;
        try {
            sd2 = SchemaReader.importSchemas(ImmutableList.of(json), new HashIdGenerator("MD5"));
        } catch (NoSuchAlgorithmException ex) {
            Assert.fail(ex.getMessage());
        }

        Assert.assertNotNull(sd2.getId("core"));
        Assert.assertNotNull(sd2.getId("extent"));
    }

    private TreeSet<Address> generateAddresses(int n) {
        TreeSet<Address> addresses = new TreeSet<Address>();
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

    private Key randomKeySuffix(int size) {
        int s = size;
        if (size == -1) {
            s = Math.abs(RAND.nextInt(1000));
        }
        byte[] bytes = new byte[s + 1];
        RAND.nextBytes(bytes);
        return new Key(bytes);
    }

    private Key randomKey(int size, Set<ByteBuffer> schemaIds) {
        ByteBuffer schemaId = null;
        int index = 0;
        int rpos = RAND.nextInt(schemaIds.size());
        for (ByteBuffer id : schemaIds) {
            if (index == rpos) {
                schemaId = id;
                break;
            }
            index++;
        }
        Key schemaKey = new Key(schemaId);
        Key suffix = randomKeySuffix(size);
        return schemaKey.append(suffix).get();
    }
}
