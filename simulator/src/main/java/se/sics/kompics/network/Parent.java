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

import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Init;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;

/**
 *
 * @author lkroll
 */
public class Parent extends ComponentDefinition {
    public Parent() {
        Component sender = create(Sender.class, Init.NONE);
        Component receiver = create(Receiver.class, Init.NONE);
        Component senderNetty = create(NettyNetwork.class, new NettyInit(MsgSizeTest.senderAddr));
        Component receiverNetty = create(NettyNetwork.class, new NettyInit(MsgSizeTest.receiverAddr));
        connect(sender.getNegative(Network.class), senderNetty.getPositive(Network.class));
        connect(receiver.getNegative(Network.class), receiverNetty.getPositive(Network.class));
    }
}
