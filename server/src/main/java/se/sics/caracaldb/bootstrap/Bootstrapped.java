/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.bootstrap;

import com.google.common.collect.ImmutableSet;
import se.sics.caracaldb.global.LookupTable;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Bootstrapped extends Event {
    
    public final LookupTable lut;
    public final ImmutableSet<Address> failedHosts;
    public final ImmutableSet<Address> joinedHosts;
    
    public Bootstrapped(LookupTable lut) {
        this(lut, null, null);
    }
    
    public Bootstrapped(LookupTable lut, ImmutableSet<Address> failedHosts, ImmutableSet<Address> joinedHosts) {
        this.lut = lut;
        this.failedHosts = failedHosts;
        this.joinedHosts = joinedHosts;
    }
}
