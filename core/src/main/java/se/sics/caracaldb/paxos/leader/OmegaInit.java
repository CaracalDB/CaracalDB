/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos.leader;

import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class OmegaInit extends Init<Omega> {
    public final long delta;
    public final long timeDelay;
    public final Address self;
    
    public OmegaInit(long delta, long timeDelay, Address self) {
        this.delta = delta;
        this.timeDelay = timeDelay;
        this.self = self;
    }
}
