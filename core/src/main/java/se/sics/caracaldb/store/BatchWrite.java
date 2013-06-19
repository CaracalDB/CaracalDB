/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import java.util.Map.Entry;
import java.util.SortedMap;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.persistence.Batch;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class BatchWrite extends StorageRequest {
    
    private final SortedMap<Key, byte[]> data;
    
    public BatchWrite(SortedMap<Key, byte[]> data) {
        this.data = data;
    }

    @Override
    public Response execute(Persistence store) {
        Batch wb = null;
        try {
            wb = store.createBatch();
            for (Entry<Key, byte[]> e : data.entrySet()) {
                wb.put(e.getKey().getArray(), e.getValue());
            }
            store.writeBatch(wb);
        } finally {
            wb.close();
        }
        return null;
    }
    
}
