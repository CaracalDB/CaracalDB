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
public abstract class ForwardToAny extends Event {
    
    public final Key key;
    
    /**
     * Return a message with desired contents and with the given address as dest.
     * 
     * @param dest Address where message is forwarded to
     * @return Message to be forwarded
     */
    abstract public Message insertDestination(Address dest);
    
    public ForwardToAny(Key dest) {
        this.key = dest;
    }
}
