/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.linearisable;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.caracaldb.store.StorageRequest;
import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
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
    public Response execute(Persistence store) throws IOException {
        for (StorageRequest req : reqs) {
            req.execute(store);
        }
        return new SnapshotResp(this, snapshotId);
    }
    
}
