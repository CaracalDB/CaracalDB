/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.kompics.Init;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.VirtualNetworkChannel;

/**
 *
 * 
 * @author Lars Kroll <lkroll@sics.se>
 */
public class HostManagerInit extends Init<HostManager> {
    
    public final Configuration config;
    public final Address netSelf;
    public final VirtualNetworkChannel vnc;
    
    public HostManagerInit(Configuration config, Address netSelf, VirtualNetworkChannel vnc) {
        this.config = config;
        this.netSelf = netSelf;
        this.vnc = vnc;
    }
    
}
