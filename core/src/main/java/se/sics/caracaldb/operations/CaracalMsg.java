/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.operations;

import se.sics.caracaldb.global.Forwardable;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class CaracalMsg extends Message implements Forwardable {
    
    public final CaracalOp op;
    
    public CaracalMsg(Address src, Address dst, CaracalOp op) {
        super(src, dst);
        this.op = op;
    }

    @Override
    public Message insertDestination(Address dest) {
        return new CaracalMsg(this.getSource(), dest, op);
    }
    
    @Override
    public String toString() {
        return "CaracalMsg(" 
                + this.getSource().toString() + ", "
                + this.getDestination().toString() + ", "
                + op.toString() + ")";
    }
}
