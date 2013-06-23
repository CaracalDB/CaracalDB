/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Set;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Sample extends Message {
    
    // Should be Immutable, but tell that to Kryo -.-
    public final Set<Address> nodes;
    
    public Sample(Address src, Address dest, ImmutableSet<Address> nodes) {
        super(src, dest);
        this.nodes = new HashSet<Address>(nodes);
    }
}
