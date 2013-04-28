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
public class VirtualSharedComponents {

    private VirtualNetworkChannel net;
    private byte[] id;
    private Address self;

    public VirtualSharedComponents(byte[] id) {
        this.id = id;
    }

    void setSelf(Address self) {
        this.self = self;
    }

    public Address getSelf() {
        return self;
    }

    void setNetwork(VirtualNetworkChannel vnc) {
        this.net = vnc;
    }

    public void connectNetwork(Component c) {
        net.addConnection(id, c.getNegative(Network.class));
    }

    public void disconnectNetwork(Component c) {
        net.removeConnection(id, c.getNegative(Network.class));
    }

    public byte[] getId() {
        return id;
    }
}
