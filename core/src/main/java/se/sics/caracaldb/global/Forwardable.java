/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface Forwardable {
    /**
     * Return a message with desired contents and with the given address as dest.
     * 
     * @param dest Address where message is forwarded to
     * @return Message to be forwarded
     */
    public Message insertDestination(Address dest);
}
