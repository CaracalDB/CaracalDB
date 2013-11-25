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
package se.sics.caracaldb.persistence;

import com.google.common.io.Closer;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.persistence.disk.LevelDBJNI;
import se.sics.caracaldb.persistence.memory.InMemoryDB;
import se.sics.caracaldb.store.GetReq;
import se.sics.caracaldb.store.GetResp;
import se.sics.caracaldb.store.Put;
import se.sics.caracaldb.store.RangeReq;
import se.sics.caracaldb.store.RangeResp;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.Launcher;

/**
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
@RunWith(JUnit4.class)
public class DBTest {

    private static final int NUM = 50;
    private static final int OFFSET = 15;

    @Test
    public void memoryDBTest() throws IOException {
        dbTest(new InMemoryDB());
    }

    @Test
    public void levelDBTest() throws IOException {
        Launcher.reset();
        Configuration conf = Launcher.config().finalise();
        String dbPath = conf.getString("caracal.database.path");
        int dbCache = 100;
        dbTest(new LevelDBJNI(dbPath, dbCache));
        cleanUp(conf.getString("caracal.database.pathHead"));
    }

    private void cleanUp(String dbPath) {
        File dbDir = new File(dbPath);
        if (dbDir.exists() && dbDir.isDirectory()) {
            if (!removeDirectory(dbDir)) {
                throw new RuntimeException("Unable to clean DB directory");
            }
        }
    }

    //TODO Alex - not symlink safe, replace with java implementation once we move to java 7+
    public static boolean removeDirectory(File dbDir) {
        if (dbDir == null) {
            return false;
        }
        if (!dbDir.exists()) {
            return false;
        }
        if (!dbDir.isDirectory()) {
            return false;
        }

        File[] childrenFiles = dbDir.listFiles();
        if (childrenFiles == null) {
            return false;
        }
        for (File file : childrenFiles) {
            if (file.isDirectory()) {
                if (!removeDirectory(file)) {
                    return false;
                }
            } else {
                if (!file.delete()) {
                    return false;
                }
            }
        }
        return dbDir.delete();
    }

    private void dbTest(Database db) throws IOException {
        try {

            Key[] keys = linearKeys(NUM);

            // PUT
            for (int i = 0; i < NUM; i++) {
                db.put(keys[i].getArray(), keys[i].getArray());
            }

            // GET
            for (int i = 0; i < NUM; i++) {
                byte[] val = db.get(keys[i].getArray());
                assertNotNull(val);
                Key k = new Key(val);
                assertEquals(keys[i], k);
            }

            // DELETE
            for (int i = 0; i < NUM; i++) {
                db.delete(keys[i].getArray());
                byte[] val = db.get(keys[i].getArray());
                assertNull(val);
            }

            // BATCH
            Batch b = db.createBatch();
            for (int i = 0; i < NUM; i++) {
                b.put(keys[i].getArray(), keys[i].getArray());
            }
            db.writeBatch(b);

            // GET for batch
            for (int i = 0; i < NUM; i++) {
                byte[] val = db.get(keys[i].getArray());
                assertNotNull(val);
                Key k = new Key(val);
                assertEquals(keys[i], k);
            }

            // ITERATOR
            Closer closer = Closer.create();
            try {
                StoreIterator it = closer.register(db.iterator());
                int i = 0;
                for (; it.hasNext(); it.next()) {
                    byte[] key = it.peekKey();
                    byte[] val = it.peekValue();
                    assertNotNull(key);
                    assertNotNull(val);
                    Key keyK = new Key(key);
                    Key valK = new Key(val);
                    //System.out.println("Step" + i + " " + keyK + " -> " + valK);
                    assertEquals(keys[i], keyK);
                    assertEquals(keys[i], valK);
                    i++;
                }
                assertEquals(NUM, i);
            } catch (Throwable e) {
                closer.rethrow(e);
            } finally {
                closer.close();
            }

            closer = Closer.create();
            try {
                int i = OFFSET;
                StoreIterator it = closer.register(db.iterator(keys[OFFSET].getArray()));
                for (; it.hasNext(); it.next()) {
                    byte[] key = it.peekKey();
                    byte[] val = it.peekValue();
                    assertNotNull(key);
                    assertNotNull(val);
                    Key keyK = new Key(key);
                    Key valK = new Key(val);
                    assertEquals(keys[i], keyK);
                    assertEquals(keys[i], valK);
                    i++;
                }
                assertEquals(NUM, i);
            } catch (Throwable e) {
                closer.rethrow(e);
            } finally {
                closer.close();
            }

        } finally {
            db.close();
            // test idempotence
            db.close();
        }
    }

    @Test
    public void leveldbMessageTest() throws IOException {
        Launcher.reset();
        Configuration conf = Launcher.config().finalise();
        String dbPath = conf.getString("caracal.database.path");
        int dbCache = 100;
        rangeQueryTest(new LevelDBJNI(dbPath, dbCache));
        cleanUp(conf.getString("caracal.database.pathHead"));
    }

    private void rangeQueryTest(Database db) throws IOException {
        Key[] keys = linearKeys(20);
        for (int i = 0; i < 20; i++) {
            new Put(keys[i], ByteBuffer.allocate(4).putInt(i + 10).array()).execute(db);
        }

        GetResp getResp = (GetResp) new GetReq(keys[5]).execute(db);
        assertEquals(15, ByteBuffer.wrap(getResp.value).getInt());

        KeyRange range1 = KeyRange.closed(keys[0]).closed(keys[19]);
        RangeReq r1 = new RangeReq(range1, null, null);
        RangeResp rr1 = (RangeResp) r1.execute(db);
        assertEquals(20, rr1.result.size());
        for (int i = 0; i < 20; i++) {
            assertTrue(rr1.result.containsKey(keys[i]));
        }
        assertEquals(15, ByteBuffer.wrap(rr1.result.get(keys[5])).getInt());

        KeyRange range2 = KeyRange.closed(keys[1]).open(keys[18]);
        RangeReq r2 = new RangeReq(range2, null, null);
        RangeResp rr2 = (RangeResp) r2.execute(db);
        assertEquals(17, rr2.result.size());
        for (int i = 1; i < 18; i++) {
            assertTrue(rr2.result.containsKey(keys[i]));
        }
    }

    private Key[] linearKeys(int NUM) {
        Key[] keys = new Key[NUM];
        for (int i = 0; i < NUM; i++) {
            keys[i] = new Key(i);
        }
        return keys;
    }
}
