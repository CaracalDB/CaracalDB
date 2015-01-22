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
import java.io.RandomAccessFile;
import java.util.SortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.HostLevelDB;
import se.sics.caracaldb.persistence.StoreIterator;
import se.sics.caracaldb.utils.ByteArrayRef;
import se.sics.kompics.address.IdUtils;

// TODO finish this later

/**
 *
 * NOT READY!!!!!!!!!!!!!!
 * 
 * @author lkroll
 */
public class FileSystemDB extends HostLevelDB {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemDB.class);

    private final File dbFolder;

    public FileSystemDB(Config conf) {
        super(conf);
        this.dbFolder = new File(conf.getString("fsdb.path"));
        if (!dbFolder.canWrite() || !dbFolder.canRead()) {
            throw new RuntimeException("Caracal doesn't have sufficient rights to write in " + dbFolder.getAbsolutePath());
        }
    }

    @Override
    public void close() {
        // no need to close anything
    }

    @Override
    public void put(byte[] key, byte[] value, int version) {
        // double try wrapping is a horrible pattern -.-
        try {
            RandomAccessFile raf = null;
            try {
                File f = key2File(key, version);
                ensureExists(f, key);
                raf = new RandomAccessFile(f, "rws");
                raf.setLength(value.length);
                raf.seek(0);
                raf.write(value);
            } catch (IOException ex) {
                LOG.error("Could not perform PUT operation: ", ex);
            } finally {
                if (raf != null) {
                    raf.close();
                }
            }
        } catch (IOException ex) {
            LOG.error("Could not perform PUT operation: ", ex);
        }
    }

    @Override
    public void replace(byte[] key, ByteArrayRef value) {
        // not sure if this makes sense in this storage format^^
        
    }

    @Override
    public void delete(byte[] key, int version) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int deleteVersions(byte[] key, int version) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ByteArrayRef get(byte[] key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public SortedMap<Integer, ByteArrayRef> getAllVersions(byte[] key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public byte[] getRaw(byte[] key) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Batch createBatch() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void writeBatch(Batch b) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public StoreIterator iterator() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public StoreIterator iterator(byte[] startKey) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private File key2File(byte[] key, int version) throws IOException {
        String keyStr = IdUtils.storeFormat(key);
        StringBuilder sb = new StringBuilder();
        sb.append(dbFolder.getCanonicalPath());
        sb.append(File.pathSeparatorChar);
        sb.append(keyStr.substring(0, 4));
        sb.append(File.pathSeparatorChar);
        sb.append(keyStr);
        sb.append('v');
        sb.append(version);
        sb.append(".val");
        return new File(sb.toString());
    }

    private void ensureExists(File f, byte[] key) throws IOException {
        if (f.exists()) {
            return;
        }
        String dirStr = IdUtils.storeFormat(new byte[]{key[0]});
        File dirF = new File(dbFolder.getCanonicalPath() + File.pathSeparator + dirStr);
        if (!dirF.exists()) {
            dirF.mkdir();
        }
        f.createNewFile();
    }
}
