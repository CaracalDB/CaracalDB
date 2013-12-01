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
package se.sics.caracaldb.replication.linearisable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.store.Diff;
import se.sics.caracaldb.store.StorageRequest;
import se.sics.caracaldb.store.StorageResponse;

/**
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class SnapshotReq extends StorageRequest {

    private List<StorageRequest> reqs = new LinkedList<StorageRequest>();
    public final long snapshotId;

    public SnapshotReq(long highestPos) {
        snapshotId = highestPos;
    }

    public void addReq(StorageRequest req) {
        reqs.add(req);
    }

    @Override
    public StorageResponse execute(Persistence store) throws IOException {
        long size = 0;
        long keys = 0;
        boolean reset = false;

        for (StorageRequest req : reqs) {
            StorageResponse res = req.execute(store);
            if (res.diff != null) {
                if (res.diff.reset) {
                    size = res.diff.size;
                    keys = res.diff.keys;
                    reset = true;
                } else {
                    size += res.diff.size;
                    keys += res.diff.keys;
                }
            }
        }
        return new SnapshotResp(this, new Diff(size, keys, reset), snapshotId);
    }

}
