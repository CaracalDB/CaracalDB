/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.vhostfd;

import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class VEPFDInit extends Init {
    public final long minRto, livePingInterval, deadPingInterval, pongTimeoutIncrement;
    public final Address self;
    
    public VEPFDInit(Address self, long minRto, long livePingInterval, long deadPingInterval, long pongTimeoutIncrement) {
        this.self = self;
        this.minRto = minRto;
        this.livePingInterval = livePingInterval;
        this.deadPingInterval = deadPingInterval;
        this.pongTimeoutIncrement = pongTimeoutIncrement;
    }
}
