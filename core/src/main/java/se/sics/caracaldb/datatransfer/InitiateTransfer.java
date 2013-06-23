/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class InitiateTransfer extends TransferMessage {
    
    public InitiateTransfer(Address src, Address dst, long id) {
        super(src, dst, id);
    }
}
