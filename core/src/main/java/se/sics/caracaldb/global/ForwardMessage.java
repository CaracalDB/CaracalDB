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
package se.sics.caracaldb.global;

import se.sics.caracaldb.Key;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Transport;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ForwardMessage implements Msg, Forwardable<ForwardMessage> {

    public final Address src;
    public final Address dst;
    public final Address orig;
    public final Transport protocol;

    public final Key forwardTo;
    public final Forwardable msg;

    public ForwardMessage(Address src, Address dst, Key forwardTo, Forwardable msg) {
        this(src, dst, src, Transport.TCP, forwardTo, msg);
    }

    public ForwardMessage(Address src, Address dst, Address orig, Transport protocol, Key forwardTo, Forwardable msg) {
        this.src = src;
        this.dst = dst;
        this.orig = orig;
        this.protocol = protocol;
        assert (forwardTo != null); // or bad things happen -.-
        this.forwardTo = forwardTo;
        this.msg = msg;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ForwardMessage(");
        sb.append(src.toString());
        sb.append(" -> ");
        sb.append(dst.toString());
        sb.append(" from ");
        sb.append(orig.toString());
        sb.append(" over ");
        sb.append(protocol.name());
        sb.append(" forward to ");
        sb.append(forwardTo.toString());
        sb.append(": \n     ");
        sb.append(msg.toString());
        sb.append("\n)");
        return sb.toString();
    }

    @Override
    public Address getSource() {
        return src;
    }

    @Override
    public Address getDestination() {
        return dst;
    }

    @Override
    public Address getOrigin() {
        return orig;
    }

    @Override
    public Transport getProtocol() {
        return protocol;
    }

    @Override
    public ForwardMessage insertDestination(Address src, Address dest) {
        return new ForwardMessage(src, dest, orig, protocol, forwardTo, msg);
    }
}
