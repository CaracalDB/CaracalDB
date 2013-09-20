/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.leader;

import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class OmegaInit extends Init<Omega> {
    public final Address self;
    
    public OmegaInit(Address self) {
        this.self = self;
    }
}
