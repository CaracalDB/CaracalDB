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
public abstract class TransferMessage extends Message {

    public final long id;

    public TransferMessage(Address src, Address dst, long id) {
        super(src, dst);
        this.id = id;
    }
}
