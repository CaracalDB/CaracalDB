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

import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Transport;

/**
 *
 * @author lkroll
 */
public class BaseDataMessage implements Msg<Address, DataHeader> {

    public final DataHeader header;

    public BaseDataMessage(DataHeader header) {
        this.header = header;
    }

    public BaseDataMessage(Address src, Address dst, Address orig, Transport proto) {
        this(new DataHeader(src, dst, orig, proto));
    }

    public BaseDataMessage(Address src, Address dst, Transport proto) {
        this(new DataHeader(src, dst, src, proto));
    }

    @Override
    public DataHeader getHeader() {
        return this.header;
    }

    @Override
    public Address getSource() {
        return this.header.getSource();
    }

    @Override
    public Address getDestination() {
        return this.header.getDestination();
    }

    public Address getOrigin() {
        return this.header.getOrigin();
    }

    @Override
    public Transport getProtocol() {
        return this.header.getProtocol();
    }

}
