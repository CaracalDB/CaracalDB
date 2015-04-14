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
package se.sics.kompics.network;

import se.sics.kompics.network.test.TestAddress;
import se.sics.kompics.network.test.TestHeader;


/**
 *
 * @author lkroll
 */
public abstract class TestMessage implements Msg {
    
    private final TestHeader header;
    
    public final long id;
    
    public TestMessage(TestAddress src, TestAddress dst, Transport proto, long id) {
        this.header = new TestHeader(src, dst, proto);
        
        this.id = id;
    }
    
        @Override
    public Address getSource() {
        return this.header.getSource();
    }

    @Override
    public Address getDestination() {
        return this.header.getDestination();
    }

    @Override
    public Transport getProtocol() {
        return this.header.getProtocol();
    }
    
    @Override
    public Header getHeader() {
        return header;
    }
}
