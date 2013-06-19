/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import se.sics.caracaldb.Key;
import se.sics.caracaldb.persistence.Persistence;
import se.sics.kompics.Request;
import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class GetReq extends StorageRequest {
    public final Key key;
    
    public GetReq(Key k) {
        key = k;
    }

    @Override
    public Response execute(Persistence store) {
        byte[] val = store.get(key.getArray());
        return new GetResp(this, key, val);
    }
}
