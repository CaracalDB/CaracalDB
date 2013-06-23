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
public final class GetRequest extends CaracalOp {
    public final Key key;

    public GetRequest(long id, Key key) {
        super(id);
        this.key = key;
    }

    @Override
    public String toString() {
        return "GetRequest(" + id + ", " + key + ")";
    }
}
