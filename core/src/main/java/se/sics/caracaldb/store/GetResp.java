/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import se.sics.caracaldb.Key;
import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class GetResp extends StorageResponse {
    
    public final Key key;
    public final byte[] value;
    
    public GetResp(GetReq req, Key k, byte[] v) {
        super(req);
        key = k;
        value = v;
    }
    
}
