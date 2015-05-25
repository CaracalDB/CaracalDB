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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.flow.DataMessage;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DataReceiver extends DataTransferComponent {
  
    private static final Logger LOG = LoggerFactory.getLogger(DataReceiver.class);
    
    // Ports
    Positive<DataSink> sink = requires(DataSink.class);
    // Instance
    private final Address self;
    private final Address source;
    
    public DataReceiver(DataReceiverInit init) {
        super(init.event.id);
        self = init.event.getDestination();
        source = init.event.getSource();

        // subscriptions
        subscribe(startHandler, control);
        subscribe(dataHandler, flow);
        subscribe(writtenHandler, sink);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            trigger(new Ack(self, source, id), net);
            state = State.WAITING;
        }
    };
    Handler<DataMessage> dataHandler = new Handler<DataMessage>() {
        @Override
        public void handle(DataMessage event) {
            if (event.data.size() == 0) {
                return;
            }
            trigger(new Data.Reference(event.data, event.collector, event.isfinal), sink);
            trigger(new AllReceived(self, source, id), net);
            if (event.isfinal) {
                state = State.DONE;
            }
        }
    };
    Handler<Data.AllWritten> writtenHandler = new Handler<Data.AllWritten>(){

        @Override
        public void handle(Data.AllWritten event) {
            trigger(new Completed(id), transfer);
        }
    };
}
