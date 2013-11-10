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

import com.google.common.base.Objects;
import java.io.Serializable;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class HostAddress implements Serializable, Comparable<HostAddress> {
    public final InetAddress ip;
    public final int port;

    public HostAddress(InetAddress ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    public HostAddress(Address addr) {
        ip = addr.getIp();
        port = addr.getPort();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ip.getHostAddress());
        sb.append(':');
        sb.append(port);

        return sb.toString();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public final int hashCode() {
        final int prime = 31;
        int result;
        result = prime + ((ip == null) ? 0 : byteHashCode(ip.getAddress()));
        result = prime * result + port;
        return result;
    }

    private int byteHashCode(byte[] bytes) {
        final int prime = 47;
        int result = prime;
        for (int i = 0; i < bytes.length; i++) {
            result = prime * result + bytes[i];
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        HostAddress other = (HostAddress) obj;
        if (!Objects.equal(ip, other.ip)) {
            return false;
        }
        return Objects.equal(port, other.port);
    }

    @Override
    public int compareTo(HostAddress that) {
        ByteBuffer thisIpBytes = ByteBuffer.wrap(this.ip.getAddress()).order(
                ByteOrder.BIG_ENDIAN);
        ByteBuffer thatIpBytes = ByteBuffer.wrap(that.ip.getAddress()).order(
                ByteOrder.BIG_ENDIAN);

        int ipres = thisIpBytes.compareTo(thatIpBytes);
        if (ipres != 0) {
            return ipres;
        }

        return this.port - that.port;

    }
}
