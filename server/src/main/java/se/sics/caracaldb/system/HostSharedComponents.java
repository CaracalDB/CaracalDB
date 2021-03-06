/* 
 * This file is part of the CaracalDB distributed storage system.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.caracaldb.system;

import se.sics.caracaldb.Address;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.kompics.Component;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.virtual.VirtualNetworkChannel;
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
