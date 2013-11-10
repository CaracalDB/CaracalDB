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

import java.util.TreeMap;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.persistence.StoreIterator;
import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class RangeReq extends StorageRequest {

    public final KeyRange range;
    public final Limit limit;
    public final boolean tombstones;

    public RangeReq(KeyRange range, Limit limit, boolean tombstones) {
        this.range = range;
        this.limit = limit;
        this.tombstones = tombstones;
    }

    @Override
    public Response execute(Persistence store) {
        TreeMap<Key, byte[]> results = new TreeMap<Key, byte[]>();
        int counter = 0;
        boolean first = true;
        StoreIterator it = null;
        try {
            for (it = store.iterator(range.begin.getArray()); it.hasNext(); it.next()) {
                byte[] key = it.peekKey();
                if (range.contains(key)) {
                    results.put(new Key(key), it.peekValue());
                } else {
                    if (!first) {
                        break; // reached end of range
                    }
                }
                if (first) {
                    first = false;
                }
            }
        } finally {
            if (it != null) {
                it.close();
            }
        }
        return new RangeResp(this, results);
    }
}
