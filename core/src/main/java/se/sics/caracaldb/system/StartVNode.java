/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class StartVNode extends Message {
    
    public final byte[] nodeId;
    
    public StartVNode(Address from, Address to, byte[] nodeId) {
        super(from, to);
        
        this.nodeId = nodeId;
    }
    
}
