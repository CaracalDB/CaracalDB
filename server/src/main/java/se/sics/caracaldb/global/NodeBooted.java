/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.kompics.Event;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class NodeBooted extends Event {
    public final Address node;
    
    public NodeBooted(Address node) {
        this.node = node;
    }
    
    @Override
    public String toString() {
        return "NodeBooted(" + node.toString() + ")";
    }
}
