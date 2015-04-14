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
package se.sics.kompics.virtual.networkmodel;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import se.sics.kompics.network.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class HostAddress implements Serializable, Comparable<HostAddress>, Address {

    public final InetSocketAddress isa;

    public HostAddress(InetSocketAddress isa) {
        this.isa = isa;
    }

    public HostAddress(InetAddress ip, int port) {
        this.isa = new InetSocketAddress(ip, port);
    }

    public HostAddress(Address addr) {
        this(addr.asSocket());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(isa.getAddress().getHostAddress());
        sb.append(':');
        sb.append(isa.getPort());

        return sb.toString();
    }

    @Override
    public int hashCode() {
        int hash = 3;
        hash = 11 * hash + (this.isa != null ? this.isa.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final HostAddress other = (HostAddress) obj;
        if (this.isa != other.isa && (this.isa == null || !this.isa.equals(other.isa))) {
            return false;
        }
        return true;
    }

    @Override
    public int compareTo(HostAddress that) {
        ByteBuffer thisIpBytes = ByteBuffer.wrap(this.isa.getAddress().getAddress()).order(
                ByteOrder.BIG_ENDIAN);
        ByteBuffer thatIpBytes = ByteBuffer.wrap(that.isa.getAddress().getAddress()).order(
                ByteOrder.BIG_ENDIAN);

        int ipres = thisIpBytes.compareTo(thatIpBytes);
        if (ipres != 0) {
            return ipres;
        }

        return this.isa.getPort() - that.isa.getPort();

    }

    @Override
    public InetAddress getIp() {
        return this.isa.getAddress();
    }

    @Override
    public int getPort() {
        return this.isa.getPort();
    }

    @Override
    public InetSocketAddress asSocket() {
        return this.isa;
    }

    @Override
    public boolean sameHostAs(Address other) {
        return other.asSocket().equals(this.asSocket());
    }
}
