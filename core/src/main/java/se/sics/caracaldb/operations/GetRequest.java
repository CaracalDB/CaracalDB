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

    @Override
    public boolean affectedBy(CaracalOp op) {
        // Remember to update this when adding new ops
        if (op instanceof PutRequest) {
            PutRequest req = (PutRequest) op;
            return req.key.equals(this.key);
        }
        return false;
    }
}
