/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.fd;

import java.util.UUID;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class UnsubscribeNodeStatus extends Event {
    public final UUID requestId;
    public final Address node;
    
    public UnsubscribeNodeStatus(UUID requestId, Address node) {
        this.requestId = requestId;
        this.node = node;
    }
}
