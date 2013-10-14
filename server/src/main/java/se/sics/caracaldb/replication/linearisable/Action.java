/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.linearisable;

import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.store.StorageRequest;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface Action<T extends CaracalOp> {

    public void initiate(T op, long pos);
    
    public StorageRequest prepareSnapshot(T op);
}
