/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import com.google.common.collect.ImmutableSet;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class SampleRequest extends Message {
    
    public int n;
    
    public SampleRequest(Address src, Address dest, int n) {
        super(src, dest);
        this.n = n;
    }
    
    public Sample reply(ImmutableSet<Address> nodes) {
        return new Sample(this.getDestination(), this.getSource(), nodes);
    }
}
