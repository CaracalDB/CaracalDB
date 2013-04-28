/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class StopVNode extends Message {
    
    public StopVNode(Address from, Address to) {
        super(from, to);
    }
}
