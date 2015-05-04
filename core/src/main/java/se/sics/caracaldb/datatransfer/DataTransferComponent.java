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
package se.sics.caracaldb.datatransfer;

import java.util.UUID;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.flow.DataFlow;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timeout;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
abstract class DataTransferComponent extends ComponentDefinition {

    public enum State {

        INITIALISING,
        TRANSFERRING,
        WAITING,
        DONE;
    }

    Negative<DataTransfer> transfer = provides(DataTransfer.class);
    Positive<DataFlow> flow = requires(DataFlow.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);

    protected final UUID id;
    protected State state;

    // statistics
    protected long dataSent = 0;
    protected long itemsSent = 0;

    DataTransferComponent(UUID id) {
        this.id = id;

        subscribe(statusHandler, transfer);
    }

    Handler<StatusRequest> statusHandler = new Handler<StatusRequest>() {

        @Override
        public void handle(StatusRequest event) {
            trigger(new StatusResponse(event, id, dataSent, itemsSent, state), transfer);
        }
    };

    // internal messages and events
    public static class RetryTimeout extends Timeout {

        public RetryTimeout(SchedulePeriodicTimeout request) {
            super(request);
        }
    }

    public static class Ack extends TransferMessage {

        public Ack(Address src, Address dst, UUID id) {
            super(src, dst, id);
        }
    }

    public static class AllReceived extends TransferMessage {

        public AllReceived(Address src, Address dst, UUID id) {
            super(src, dst, id);
        }
    }

}
