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
public final class PutRequest extends CaracalOp {

    public final Key key;
    public final byte[] data;

    public PutRequest(long id, Key key, byte[] data) {
        super(id);
        this.key = key;
        this.data = data;
    }

    @Override
    public String toString() {
        return "PutRequest(" + id + ", " + key + ", " + (new Key(data)).toString() + ")";
    }
}
