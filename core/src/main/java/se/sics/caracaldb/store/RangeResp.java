/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import java.util.SortedMap;
import se.sics.caracaldb.Key;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class RangeResp extends StorageResponse {
    
    public final RangeReq req;
    public final SortedMap<Key, byte[]> result;
    public final boolean readLimit;
    
    public RangeResp(RangeReq req, SortedMap<Key, byte[]> result, boolean readAll) {
        super(req);
        this.req = req;
        this.result = result;
        this.readLimit = readAll;
    }
}