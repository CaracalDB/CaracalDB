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
package se.sics.caracaldb.persistence.disk;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;

import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.Database;
import se.sics.caracaldb.persistence.StoreIterator;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class LevelDBJNI implements Database {

    private DB db;
    private final String dbPath;
    private final int cacheSize;

    /**
     * @param dbPath
     * @param cacheSize in megabytes
     * @throws IOException
     */
    public LevelDBJNI(String dbPath, int cacheSize) throws IOException {
        this.dbPath = dbPath;
        this.cacheSize = cacheSize;
        File dbDir = new File(dbPath);
        if (!dbDir.exists()) {
            if (!dbDir.mkdirs()) {
                throw new IOException("Unable to create DB directory");
            }
        }

        Options options = new Options();
        options.createIfMissing(true);
        options.cacheSize(cacheSize * 1048576);         // In MB
        options.compressionType(CompressionType.NONE);	// No compression

        db = JniDBFactory.factory.open(dbDir, options);
    }
    
    @Override
    public String toString() {
        return "LevelDBJNI(\""+dbPath+"\", "+cacheSize+")";
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        db.put(key, value);
    }

    @Override
    public void delete(byte[] key) {
        db.delete(key);
    }

    @Override
    public byte[] get(byte[] key) {
        return db.get(key);
    }

    @Override
    public Batch createBatch() {
        return new LevelDBBatch(db.createWriteBatch());
    }

    @Override
    public void writeBatch(Batch b) {
        LevelDBBatch batch = (LevelDBBatch) b;
        // If this cast doesn't work, someone is doing something seriously wrong
        db.write(batch.getLevelDBBatch());
    }

    @Override
    public StoreIterator iterator() {
        return new LevelDBIterator(db.iterator());
    }

    @Override
    public StoreIterator iterator(byte[] startKey) {
        return new LevelDBIterator(db.iterator(), startKey);
    }

    private static class LevelDBBatch implements Batch {

        private WriteBatch batch;

        private LevelDBBatch(WriteBatch batch) {
            this.batch = batch;
        }

        @Override
        public void put(byte[] key, byte[] value) {
            batch.put(key, value);
        }

        @Override
        public void delete(byte[] key) {
            batch.delete(key);
        }

        @Override
        public void close() {
            if (batch != null) {
                batch.close();
                batch = null;
            }
        }

        private WriteBatch getLevelDBBatch() {
            return batch;
        }
    }

    private static class LevelDBIterator implements StoreIterator {

        private DBIterator it;

        private LevelDBIterator(DBIterator it) {
            this.it = it;
            this.it.seekToFirst();
        }

        private LevelDBIterator(DBIterator it, byte[] startKey) {
            this.it = it;
            this.it.seek(startKey);
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public void next() {
            it.next();
        }

        @Override
        public Map.Entry<byte[], byte[]> peekNext() {
            return it.peekNext();
        }

        @Override
        public byte[] peekKey() {
            return it.peekNext().getKey();
        }

        @Override
        public byte[] peekValue() {
            return it.peekNext().getValue();
        }

        @Override
        public void close() throws IOException {
            if (it != null) {
                it.close();
                it = null;
            }
        }
    }
}