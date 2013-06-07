/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.bootstrap;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class BootstrapResponse extends Message {
    
    public final byte[] lut;
    
    public BootstrapResponse(Address src, Address dest, byte[] lut) {
        super(src, dest);
        this.lut = lut;
    }
}
