/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.operations;

import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class CaracalResponse extends CaracalOp {

    public final ResponseCode code;

    public CaracalResponse(long id, ResponseCode code) {
        super(id);
        this.code = code;
    }

    @Override
    public String toString() {
        return "EmptyResponse(" + id + ", " + code.name() + ")";
    }
}
