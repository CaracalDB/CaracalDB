/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.kompics.Component;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.VirtualNetworkChannel;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class HostSharedComponents {
    private Positive<Timer> timer;
    private VirtualNetworkChannel net;
    private Address self;
    
    void setNetwork(VirtualNetworkChannel vnc) {
        this.net = vnc;
    }
    
    void setSelf(Address self) {
        this.self = self;
    }
    
    void setTimer(Positive<Timer> timer) {
        this.timer = timer;
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
    
    public Positive<Timer> getTimer() {
        return timer;
    }
}
