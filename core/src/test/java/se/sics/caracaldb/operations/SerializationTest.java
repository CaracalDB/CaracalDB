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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.caracaldb.CoreSerializer;
import se.sics.caracaldb.Key;
import se.sics.kompics.address.Address;

/**
 *
 * @author lkroll
 */
@RunWith(JUnit4.class)
public class SerializationTest {

    @Test
    public void messageTest() throws UnknownHostException {
        InetAddress ip = InetAddress.getLocalHost();
        Address source = new Address(ip, 1234, "abcd".getBytes());
        Address dest = new Address(ip, 5678, "efgh".getBytes());
        
        Key k = Key.fromHex("1F 2F 3F 4F");
        byte[] data = "Some Data".getBytes();

        ByteBuf buf = Unpooled.buffer();

        OperationSerializer opS = CoreSerializer.OP.instance;
        
        // PUT
        // REQ
        PutRequest pr = new PutRequest(1234l, k, data);
        CaracalMsg msg = new CaracalMsg(source, dest, pr);
        opS.toBinary(msg, buf);
        //TODO finish
        buf.clear();
        
        buf.release();
    }
}
