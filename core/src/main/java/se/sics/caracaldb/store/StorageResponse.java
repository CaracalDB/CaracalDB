/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class StorageResponse extends Response {
    
    private long id;
    
    StorageResponse(StorageRequest req) {
        super(req);
        id = req.getId();
    }
    
    /**
     * Set optional id to match up requests
     * 
     * @param val 
     */
    public long getId() {
        return id;
    }
}
