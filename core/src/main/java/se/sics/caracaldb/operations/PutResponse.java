/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.operations;

import se.sics.caracaldb.Key;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public final class PutResponse extends CaracalResponse {
    public final Key key;

    public PutResponse(long id, Key key) {
        this(id, key, ResponseCode.SUCCESS);
    }
    
    public PutResponse(long id, Key key, ResponseCode code) {
        super(id, code);
        this.key = key;
    }
    
    @Override
    public String toString() {
        String str = "PutResponse(";
        str += id + ", ";
        str += key.toString() + ", ";
        str += code.name() + ")";
        return str;
    }
}
