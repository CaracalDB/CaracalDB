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
package se.sics.caracaldb.store;

import com.google.common.io.Closer;
import java.io.IOException;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.persistence.StoreIterator;

/**
 *
 * @author lkroll
 */
public class CleanupReq extends StorageRequest {

    public final KeyRange range;
    public final int versionId;

    public CleanupReq(KeyRange range, int versionId) {
        this.range = range;
        this.versionId = versionId;
    }

    @Override
    public StorageResponse execute(Persistence store) throws IOException {
        long size = 0;
        long keys = 0;

        Closer closer = Closer.create();
        Batch b;
        try {
            byte[] begin = range.begin.getArray();
            b = closer.register(store.createBatch());
            for (StoreIterator it = closer.register(store.iterator(begin)); it.hasNext(); it.next()) {
                byte[] key = it.peekKey();
                if (range.contains(key)) {
                    int l = b.deleteVersions(key, versionId);
                    if (l > 0) {
                        keys++;
                        size += key.length;
                        size += l;
                    }
                } else {
                    //special case (a,b) and key is a
                    if (Key.compare(begin, key) != 0) {
                        break; // reached end of range
                    }
                }
            }
            store.writeBatch(b);
        } catch (Throwable e) {
            closer.rethrow(e);
        } finally {
            closer.close();
        }
        return new SizeResp(this, new Diff(size, keys, true));
    }

}
