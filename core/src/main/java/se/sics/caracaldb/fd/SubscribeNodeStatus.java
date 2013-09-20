/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.fd;

import java.util.UUID;
import se.sics.kompics.Event;
import se.sics.kompics.Request;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class SubscribeNodeStatus extends Request {
    public final UUID requestId = UUID.randomUUID();
    public final Address node;
    
    public SubscribeNodeStatus(Address node) {
        this.node = node;
    }
}
