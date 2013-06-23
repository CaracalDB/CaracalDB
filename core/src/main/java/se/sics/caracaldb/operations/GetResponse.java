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
public final class GetResponse extends CaracalResponse {
    public final Key key;
    public final byte[] data;

    public GetResponse(long id, Key key, byte[] data) {
        this(id, key, data, ResponseCode.SUCCESS);
    }
    
    public GetResponse(long id, Key key, byte[] data, ResponseCode code) {
        super(id, code);
        this.key = key;
        this.data = data;
    }
    
    @Override
    public String toString() {
        String str = "GetResponse(";
        str += id + ", ";
        str += key.toString() + ", ";
        str += (new Key(data)).toString() + ", ";
        str += code.name() + ")";
        return str;
    }
}
