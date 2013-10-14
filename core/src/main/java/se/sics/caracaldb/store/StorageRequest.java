/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import java.io.IOException;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.kompics.Request;
import se.sics.kompics.Response;

/**
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public abstract class StorageRequest extends Request {
    
    private long id;
    
    /**
     * Custom interface for local storage queries.
     * 
     * Implement the execute function, to do whatever operation you need 
     * to be done on the persistent storage medium.
     * You are guaranteed exclusive access for the duration of the operation.
     * 
     * ATTENTION: Do NOT pass the reference to the store to some other parent
     * object and access it later directly!!!
     * For performance reasons the reference is not temporary and not synchronised,
     * i.e. you WILL be doing parallel access to the database if you try this.
     * 
     * @param store a reference to the backing store
     * @return either a response or null if no response is required
     */
    public abstract Response execute(Persistence store) throws IOException;
    
    /**
     * Set optional id to match up requests
     * 
     * @param val 
     */
    public void setId(long val) {
        this.id = val;
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