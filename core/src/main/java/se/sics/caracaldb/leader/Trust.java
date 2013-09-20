/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.leader;

import se.sics.kompics.Event;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Trust extends Event {
    public final Address leader;
    
    public Trust(Address leader) {
        this.leader = leader;
    }
}
