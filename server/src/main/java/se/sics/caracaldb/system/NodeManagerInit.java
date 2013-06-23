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
public class NodeManagerInit extends Init<NodeManager> {
    
    public final VirtualSharedComponents vsc;
    public final Configuration config;
    
    public NodeManagerInit(VirtualSharedComponents vsc, Configuration config) {
        this.vsc = vsc;
        this.config = config;
    }
}
