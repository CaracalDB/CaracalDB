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

import java.util.Random;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;

/**
 *
 * @author lkroll
 */
public class Sender extends ComponentDefinition {

    Positive<Network> net = requires(Network.class);
    
    private long id = 0;
    private int size = MsgSizeTest.MIN_SIZE;
    private final Random rand = new Random(0);

    public Sender() {
        subscribe(startHandler, control);
        subscribe(ackHandler, net);
    }

    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            sendMessage();
        }
    };
    Handler<DataMessage.Ack> ackHandler = new Handler<DataMessage.Ack>() {

        @Override
        public void handle(DataMessage.Ack event) {
            sendMessage();
        }
    };

    private void sendMessage() {
        MsgSizeTest.LOG.info("Preparing new message of size {}.", size);
        byte[] data = new byte[size];
        rand.nextBytes(data);
        trigger(new DataMessage(MsgSizeTest.senderAddr, MsgSizeTest.receiverAddr, MsgSizeTest.PROTOCOL, id, data), net);
        MsgSizeTest.LOG.info("Sending message {} of size {}.", id, size);
        id++;
        size += MsgSizeTest.INCREMENT;
    }
}
