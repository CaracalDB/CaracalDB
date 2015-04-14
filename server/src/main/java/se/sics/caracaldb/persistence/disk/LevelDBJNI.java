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

import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.util.SortedMap;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.WriteBatch;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.HostLevelDB;
import se.sics.caracaldb.persistence.MultiVersionUtil;
import se.sics.caracaldb.persistence.StoreIterator;
import com.larskroll.common.ByteArrayRef;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class LevelDBJNI extends HostLevelDB {

    private DB db;
    private final String dbPath;
    private final long cacheSize;

    /**
     * @param config
     * @throws IOException
     */
    public LevelDBJNI(Config config) throws IOException {
        super(config);
        this.dbPath = config.getString("leveldb.path");
        this.cacheSize = config.getBytes("leveldb.cache");
        File dbDir = new File(dbPath);
        if (!dbDir.exists()) {
            if (!dbDir.mkdirs()) {
                throw new IOException("Unable to create DB directory");
            }
        }

        Options options = new Options();
        options.createIfMissing(true);
        options.cacheSize(cacheSize);
        options.compressionType(CompressionType.NONE);	// No compression

        db = JniDBFactory.factory.open(dbDir, options);
    }

    @Override
    public String toString() {
        return "LevelDBJNI(\"" + dbPath + "\", " + cacheSize + ")";
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            db = null;
        }
    }

    @Override
    public void put(byte[] key, byte[] value, int version) {
        SortedMap<Integer, ByteArrayRef> versions = getAllVersions(key);
        versions.put(version, new ByteArrayRef(0, value.length, value));
        db.put(key, MultiVersionUtil.pack(versions));
    }

    @Override
    public void delete(byte[] key, int version) {
        SortedMap<Integer, ByteArrayRef> versions = getAllVersions(key);
        if (versions.isEmpty()) {
            return; // nothing to do
        }
        versions.put(version, new ByteArrayRef(0, 0, null));
        byte[] data = MultiVersionUtil.pack(versions);
        if (data == null) {
            db.delete(key);
        } else {
            db.put(key, data);
        }
    }

    @Override
    public ByteArrayRef get(byte[] key) {
        SortedMap<Integer, ByteArrayRef> versions = getAllVersions(key);
        if (versions.isEmpty()) {
            return null;
        }
        return versions.get(versions.firstKey());
    }

    @Override
    public Batch createBatch() {
        return new LevelDBBatch(db.createWriteBatch(), this);
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

    @Override
    public void replace(byte[] key, ByteArrayRef value) {
        db.put(key, value.getBackingArray());
    }

    @Override
    public int deleteVersions(byte[] key, int version) {
        SortedMap<Integer, ByteArrayRef> versions = getAllVersions(key);
        if (versions.isEmpty()) {
            return 0; // nothing to do
        }
        SortedMap<Integer, ByteArrayRef> newVersions = versions.headMap(version);
        if (newVersions.isEmpty()) { // always retain the newest value
            newVersions.put(versions.firstKey(), versions.get(versions.firstKey()));
        }
        byte[] newData = MultiVersionUtil.pack(newVersions);
        if (newData == null) {
            db.delete(key);
            return 0;
        }
        db.put(key, newData);
        return newData.length;

    }

    @Override
    public SortedMap<Integer, ByteArrayRef> getAllVersions(byte[] key) {
        byte[] data = db.get(key);
        SortedMap<Integer, ByteArrayRef> versions = MultiVersionUtil.unpack(data);
        return versions;
    }

    @Override
    public byte[] getRaw(byte[] key) {
        return db.get(key);
    }

    private static class LevelDBBatch implements Batch {

        private WriteBatch batch;
        private LevelDBJNI db;

        private LevelDBBatch(WriteBatch batch, LevelDBJNI db) {
            this.batch = batch;
            this.db = db;
        }

        @Override
        public void put(byte[] key, byte[] value, int version) {
            SortedMap<Integer, ByteArrayRef> versions = db.getAllVersions(key);
            versions.put(version, new ByteArrayRef(0, value.length, value));
            batch.put(key, MultiVersionUtil.pack(versions));
        }

        @Override
        public void delete(byte[] key, int version) {
            SortedMap<Integer, ByteArrayRef> versions = db.getAllVersions(key);
            if (versions.isEmpty()) {
                return; // nothing to do
            }
            versions.put(version, new ByteArrayRef(0, 0, null));
            byte[] data = MultiVersionUtil.pack(versions);
            if (data == null) {
                batch.delete(key);
            } else {
                batch.put(key, data);
            }
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

        @Override
        public void replace(byte[] key, ByteArrayRef value) {
            batch.put(key, value.getBackingArray());
        }

        @Override
        public int deleteVersions(byte[] key, int version) {
            SortedMap<Integer, ByteArrayRef> versions = db.getAllVersions(key);
            if (versions.isEmpty()) {
                return 0; // nothing to do
            }
            SortedMap<Integer, ByteArrayRef> newVersions = versions.headMap(version);
            if (newVersions.isEmpty()) { // always retain the newest value
                newVersions.put(versions.firstKey(), versions.get(versions.firstKey()));
            }
            byte[] newData = MultiVersionUtil.pack(newVersions);
            if (newData == null) {
                batch.delete(key);
                return 0;
            }
            batch.put(key, newData);
            return newData.length;
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
        public byte[] peekKey() {
            return it.peekNext().getKey();
        }

        @Override
        public ByteArrayRef peekValue() {
            SortedMap<Integer, ByteArrayRef> versions = peekAllValues();
            if (versions.isEmpty()) {
                return null;
            }
            return versions.get(versions.firstKey());
        }

        @Override
        public void close() throws IOException {
            if (it != null) {
                it.close();
                it = null;
            }
        }

        @Override
        public SortedMap<Integer, ByteArrayRef> peekAllValues() {
            return MultiVersionUtil.unpack(peekRaw());
        }

        @Override
        public byte[] peekRaw() {
            return it.peekNext().getValue();
        }
    }
}
