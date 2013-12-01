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
    public StorageResponse execute(Persistence store) {
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
        //TODO BatchWrites don't generate a diff (would be very inefficient)
        // If you use BatchWrites make sure to run a size scan afterwards (for example after syncing up nodes)
    }
    
}
