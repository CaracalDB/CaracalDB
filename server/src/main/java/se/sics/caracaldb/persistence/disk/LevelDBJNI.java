/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.persistence.disk;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
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

    /**
     * @param dbPath
     * @param cacheSize in megabytes
     * @throws IOException
     */
    public LevelDBJNI(String dbPath, int cacheSize) throws IOException {
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