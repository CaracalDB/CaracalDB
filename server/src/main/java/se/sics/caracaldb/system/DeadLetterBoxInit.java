/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DeadLetterBoxInit extends Init<DeadLetterBox> {
    public final Address self;
    
    public DeadLetterBoxInit(Address self) {
        this.self = self;
    }
    
}
