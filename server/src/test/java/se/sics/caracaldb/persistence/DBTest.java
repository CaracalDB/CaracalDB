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

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.persistence.memory.InMemoryDB;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
@RunWith(JUnit4.class)
public class DBTest {

    private static final int NUM = 50;
    private static final int OFFSET = 15;

    @Test
    public void memoryDBTest() {
        dbTest(new InMemoryDB());
    }

    private void dbTest(Database db) {
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
            StoreIterator it = null;
            try {
                int i = 0;
                for (it = db.iterator(); it.hasNext(); it.next()) {
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
            } finally {
                if (it != null) {
                    it.close();
                }
            }
            
            try {
                int i = OFFSET;
                for (it = db.iterator(keys[OFFSET].getArray()); it.hasNext(); it.next()) {
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
            } finally {
                if (it != null) {
                    it.close();
                }
            }
            
        } finally {
            db.close();
            // test idempotence
            db.close();
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
