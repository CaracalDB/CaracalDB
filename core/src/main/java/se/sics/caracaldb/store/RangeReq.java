/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
