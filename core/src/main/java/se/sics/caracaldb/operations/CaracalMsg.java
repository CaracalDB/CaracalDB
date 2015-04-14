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
package se.sics.caracaldb.operations;

import java.util.UUID;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.BaseMessage;
import se.sics.caracaldb.global.Forwardable;
import se.sics.kompics.network.Transport;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class CaracalMsg extends BaseMessage implements Forwardable<CaracalMsg> {

    public final CaracalOp op;
    public final long lutversion;

    public CaracalMsg(Address src, Address dst, CaracalOp op) {
        this(src, dst, src, Transport.TCP, op, -1);
    }

    public CaracalMsg(Address src, Address dst, Address orig, CaracalOp op) {
        this(src, dst, orig, Transport.TCP, op, -1);
    }

    public CaracalMsg(Address src, Address dst, Address orig, Transport protocol, CaracalOp op, long lutversion) {
        super(src, dst, orig, protocol);
        this.op = op;
        this.lutversion = lutversion;
    }

    @Override
    public CaracalMsg insertDestination(Address src, Address dest, long lutversion) {
        return new CaracalMsg(src, dest, this.getOrigin(), this.getProtocol(), op, lutversion);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("CaracalMsg(");
        sb.append(this.getSource().toString());
        sb.append(" -> ");
        sb.append(this.getDestination().toString());
        sb.append(" from ");
        sb.append(this.getOrigin().toString());
        sb.append(" over ");
        sb.append(this.getProtocol().name());
        sb.append(" with:");
        sb.append(op.toString());
        sb.append(")");
        return sb.toString();
    }

    @Override
    public UUID getId() {
        return op.id;
    }

    @Override
    public long getLUTVersion() {
        return this.lutversion;
    }
}
