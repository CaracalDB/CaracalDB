/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import java.util.SortedMap;
import se.sics.caracaldb.Key;
import se.sics.kompics.Response;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class RangeResp extends Response {
    
    public final SortedMap<Key, byte[]> result;
    public final RangeReq req;
    
    public RangeResp(RangeReq req, SortedMap<Key, byte[]> result) {
        super(req);
        this.req = req;
        this.result = result;
    }
    
}
