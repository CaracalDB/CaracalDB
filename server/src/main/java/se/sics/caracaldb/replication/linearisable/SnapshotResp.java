/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.linearisable;

import se.sics.caracaldb.store.StorageResponse;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class SnapshotResp extends StorageResponse {
    
    public final long snapshotId;
    
    public SnapshotResp(SnapshotReq req, long pos) {
        super(req);
        snapshotId = pos;
    }
}
