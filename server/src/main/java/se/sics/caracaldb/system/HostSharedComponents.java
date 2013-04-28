/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.kompics.Component;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.VirtualNetworkChannel;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class HostSharedComponents {
    private VirtualNetworkChannel net;
    private Address self;
    
    void setNetwork(VirtualNetworkChannel vnc) {
        this.net = vnc;
    }
    
    void setSelf(Address self) {
        this.self = self;
    }
    
    
    public void connectNetwork(Component c) {
        net.addConnection(null, c.getNegative(Network.class));
    }
    
    public void disconnectNetwork(Component c) {
        net.removeConnection(null, c.getNegative(Network.class));
    }
    
    public Address getSelf() {
        return self;
    }
    
    VirtualNetworkChannel getNet() {
        return net;
    }
}
