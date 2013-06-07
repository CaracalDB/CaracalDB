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
public class BootstrapRequest extends Message {
    public BootstrapRequest(Address src, Address dest) {
        super(src, dest);
    }
}
