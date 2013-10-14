/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import java.util.Map;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class InitiateTransfer extends TransferMessage {
    
    public final Map<String, Object> metadata;
    
    public InitiateTransfer(Address src, Address dst, long id, Map<String, Object> metadata) {
        super(src, dst, id);
        this.metadata = metadata;
    }
}
