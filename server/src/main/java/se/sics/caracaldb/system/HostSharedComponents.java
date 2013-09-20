/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.caracaldb.fd.EventualFailureDetector;
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
public class HostSharedComponents extends ServiceRegistry {
    /*
     * Address
     */

    private Address self;

    public Address getSelf() {
        return self;
    }

    void setSelf(Address self) {
        this.self = self;
    }
    /*
     * Core services
     */
    private Positive<Timer> timer;
    private VirtualNetworkChannel net;
    private Positive<EventualFailureDetector> fd;

    void setNetwork(VirtualNetworkChannel vnc) {
        this.net = vnc;
    }

    void setTimer(Positive<Timer> timer) {
        this.timer = timer;
    }
    
    void setFailureDetector(Positive<EventualFailureDetector> fd) {
        this.fd = fd;
    }

    public void connectNetwork(Component c) {
        net.addConnection(null, c.getNegative(Network.class));
    }

    public void disconnectNetwork(Component c) {
        net.removeConnection(null, c.getNegative(Network.class));
    }

    VirtualNetworkChannel getNet() {
        return net;
    }

    public Positive<Timer> getTimer() {
        return timer;
    }
    
    public Positive<EventualFailureDetector> getFailureDetector() {
        return this.fd;
    }
    
}
