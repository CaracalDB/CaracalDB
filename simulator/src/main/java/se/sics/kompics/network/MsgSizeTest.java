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

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Kompics;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.netty.serialization.Serializers;

/**
 *
 * @author lkroll
 */
public class MsgSizeTest {
    
    public volatile static Address senderAddr;
    public volatile static Address receiverAddr;
    public static final Transport PROTOCOL = Transport.UDP;
    public static final Logger LOG = LoggerFactory.getLogger(MsgSizeTest.class);
    public static final int MIN_SIZE = 1000; // 1KB
    public static final int MAX_SIZE = 1000*1000*1000; // 1GB
    public static final int INCREMENT = 100; // 1KB
    
    public static void main(String[] args) throws UnknownHostException {
        Serializers.register(new TestSerializer(), "testS");
        Serializers.register(TestMessage.class, "testS");
        
        InetAddress ip = InetAddress.getLocalHost();
        senderAddr = new Address(ip, 45454, null);
        receiverAddr = new Address(ip, 45455, null);
        
        Kompics.createAndStart(Parent.class);
    }
}
