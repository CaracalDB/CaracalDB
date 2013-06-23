/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.caracaldb.Key;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ForwardMessage extends Message {
    public final Key forwardTo;
    public final Forwardable msg;
    
    public ForwardMessage(Address src, Address dst, Key forwardTo, Forwardable msg) {
        super(src, dst);
        this.forwardTo = forwardTo;
        this.msg = msg;
    }
    
    @Override
    public String toString() {
        String str = "ForwardMessage(";
        str += getSource().toString() + ", ";
        str += getDestination().toString() + ", ";
        str += forwardTo.toString() + ", ";
        str += msg.toString() + ")";
        return str;
    }
}
