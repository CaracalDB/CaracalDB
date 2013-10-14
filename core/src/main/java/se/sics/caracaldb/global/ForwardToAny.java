/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.caracaldb.Key;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 * 
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ForwardToAny extends Event {
    
    public final Key key;
    public final Forwardable msg;
    
    public ForwardToAny(Key dest, Forwardable msg) {
        this.key = dest;
        this.msg = msg;
    }
    
    @Override
    public String toString() {
        return "ForwardToAny(" + key.toString() + ", " + msg.toString() + ")";
    }
}