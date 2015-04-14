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
package se.sics.caracaldb;

import com.google.common.base.Objects;
import com.google.common.primitives.UnsignedBytes;
import com.larskroll.common.ByteArrayFormatter;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Comparator;

/**
 *
 * @author lkroll
 */
public class Address implements se.sics.kompics.network.virtual.Address, Serializable, Comparable<Address> {

    /**
     *
     */
    private static final long serialVersionUID = -7330046056166039992L;
    private final InetSocketAddress ipport;
    private final byte[] id;

    public Address(InetSocketAddress ipport, byte[] id) {
        this.ipport = ipport;
        this.id = id;
    }

    public Address(InetSocketAddress ipport, byte id) {
        this(ipport, new byte[]{id});
    }

    public Address(InetAddress ip, int port, byte[] id) {
        this(new InetSocketAddress(ip, port), id);

    }

    public Address(InetAddress ip, int port, byte id) {
        this(ip, port, new byte[]{id});
    }

    /**
     * Gets the ip.
     *
     * @return the ip
     */
    @Override
    public final InetAddress getIp() {
        return ipport.getAddress();
    }

    /**
     * Gets the port.
     *
     * @return the port
     */
    @Override
    public final int getPort() {
        return ipport.getPort();
    }

    /**
     * Gets the id.
     *
     * @return the id
     */
    @Override
    public final byte[] getId() {
        return id;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(ipport.getAddress().getHostAddress());
        sb.append(':');
        sb.append(ipport.getPort());
        sb.append('/');

        ByteArrayFormatter.printFormat(id, sb);

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
        result = prime + ((id == null) ? 0 : byteHashCode(id));
        result = prime * result + ((ipport == null) ? 0 : ipport.hashCode());
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
        Address other = (Address) obj;
        if (!Objects.equal(ipport, other.ipport)) {
            return false;
        }
        if (id == null) {
            if (other.id == null) {
                return true;
            }
            return false;
        }
        return Arrays.equals(id, other.id);
    }

    @Override
    public int compareTo(Address that) {
        ByteBuffer thisIpBytes = ByteBuffer.wrap(this.ipport.getAddress().getAddress()).order(
                ByteOrder.BIG_ENDIAN);
        ByteBuffer thatIpBytes = ByteBuffer.wrap(that.ipport.getAddress().getAddress()).order(
                ByteOrder.BIG_ENDIAN);

        int ipres = thisIpBytes.compareTo(thatIpBytes);
        if (ipres != 0) {
            return ipres;
        }

        if (this.ipport.getPort() != that.ipport.getPort()) {
            return this.ipport.getPort() - that.ipport.getPort();
        }

        if ((this.id == null) && (that.id == null)) {
            return 0;
        }

        if (this.id == null) {
            return -1;
        }

        if (that.id == null) {
            return 1;
        }

        return byteLexComp.compare(id, that.id);
    }
    private static Comparator<byte[]> byteLexComp = UnsignedBytes.lexicographicalComparator();

    public Address newVirtual(byte[] id) {
        return new Address(this.ipport, id); //Should be safe to reuse InetSocketAddress object
    }

    public Address newVirtual(byte id) {
        return new Address(this.ipport, id); //Should be safe to reuse InetSocketAddress object
    }

    public Address hostAddress() {
        return new Address(this.ipport, null);
    }

    @Override
    public boolean sameHostAs(se.sics.kompics.network.Address other) {
        return this.ipport.equals(other.asSocket());
    }

    public boolean sameHostAs(InetSocketAddress isa) {
        return this.ipport.equals(isa);
    }

    @Override
    public InetSocketAddress asSocket() {
        return this.ipport;
    }

    public static Address fromInetSocket(InetSocketAddress sock) {
        return new Address(sock, null);
    }
}
