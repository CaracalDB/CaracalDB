/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import se.sics.caracaldb.Key;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.kompics.Event;
import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Put extends StorageRequest {
    
    public final Key key;
    public final byte[] value;
    
    public Put(Key k, byte[] v) {
        key = k;
        value = v;
    }

    @Override
    public Response execute(Persistence store) {
        store.put(key.getArray(), value);
        return null;
    }
    
    @Override
    public String toString() {
        return "PutReq("+key+", "+value+")";
    }
}
