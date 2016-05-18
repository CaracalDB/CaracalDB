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
package se.sics.caracaldb.experiment.randomdata;

import java.util.Random;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.MessageNotify;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;

/**
 *
 * @author lkroll
 */
public class Sender extends ComponentDefinition {
    
    static final int K = 1000;
    static final int M = K * K;
    static final Logger LOG = LoggerFactory.getLogger(Sender.class);
    static final int INITIAL_Q = 1000;
    static final int DATA_POOL_SIZE = 128 * M;
    static final int DATA_SIZE = 64 * K;
    // Ports
    Positive<Network> net = requires(Network.class);
    // Instance
    private final Address self;
    private final Address target;
    //private UUID timeoutId;
    private final Random rand = new Random(1);
    private final byte[] randomData;
    private final int lastStart;
    private final Transport proto;
    
    public Sender(Init init) {
        Address parentAddress = config().getValue("experiment.self.address", Address.class);
        byte[] id = RandomDataSerializer.uuid2Bytes(this.id());
        self = parentAddress.newVirtual(id);
        target = init.target;
        proto = init.protocol;
        
        LOG.debug("Generating random pool of {}bytes...", DATA_POOL_SIZE);
        randomData = new byte[DATA_POOL_SIZE];
        rand.nextBytes(randomData);
        lastStart = randomData.length - DATA_SIZE - 1;

        // subscriptions
        subscribe(startHandler, control);
        subscribe(notifyHandler, net);
    }
    
    Handler<Start> startHandler = new Handler<Start>() {
        
        @Override
        public void handle(Start event) {
            for (int i = 0; i < INITIAL_Q; i++) {
                RandomDataMessage msg = messageFor(i);
                trigger(MessageNotify.create(msg), net);
            }
//            if (pingProtocol != null) {
//                SchedulePeriodicTimeout spt
//                        = new SchedulePeriodicTimeout(0, PINGTIME);
//                PingTimeout timeoutEvent = new PingTimeout(spt);
//                spt.setTimeoutEvent(timeoutEvent);
//                timeoutId = timeoutEvent.getTimeoutId();
//                trigger(spt, timer);
//            }
        }
    };
    
    Handler<MessageNotify.Resp> notifyHandler = new Handler<MessageNotify.Resp>() {
        
        @Override
        public void handle(MessageNotify.Resp event) {
            if (event.getState() == MessageNotify.State.SENT) {
                RandomDataMessage msg = messageFor(rand.nextInt());
                trigger(MessageNotify.create(msg), net);
            }
            if (event.getState() == MessageNotify.State.FAILED) {
                LOG.warn("Sending of message {} failed! Sending another one instead.", event.msgId);
                RandomDataMessage msg = messageFor(rand.nextInt());
                trigger(MessageNotify.create(msg), net);
            }
        }
        
    };
    
    private RandomDataMessage messageFor(int offset) {
        int start = offset % lastStart;
        byte[] data = new byte[DATA_SIZE];
        System.arraycopy(randomData, start, data, 0, DATA_SIZE);
        return new RandomDataMessage(self, target, proto, UUID.randomUUID(), data);
    }
    
    @Override
    public void tearDown() {
        //            if (timeoutId != null) {
//                trigger(new CancelPeriodicTimeout(timeoutId), timer);
//            }
    }
    
    public static final class Init extends se.sics.kompics.Init<Sender> {
        
        public final Transport protocol;
        public final Address target;
        
        public Init(Address target, Transport protocol) {
            this.target = target;
            this.protocol = protocol;
        }
    }
}
