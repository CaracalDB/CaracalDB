/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import java.io.IOException;
import java.util.SortedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.store.BatchWrite;
import se.sics.kompics.Handler;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DataReceiver extends DataTransferComponent {

    private static final Logger LOG = LoggerFactory.getLogger(DataReceiver.class);
    private final Address self;
    private final Address source;

    public DataReceiver(DataReceiverInit init) {
        super(init.event.id);
        self = init.event.getDestination();
        source = init.event.getSource();

        // subscriptions

        subscribe(startHandler, control);
        subscribe(dataHandler, net);
    }
    Handler<Start> startHandler = new Handler<Start>() {
        @Override
        public void handle(Start event) {
            trigger(new Ack(self, source, id), net);
            state = State.WAITING;
        }
    };
    Handler<Data> dataHandler = new Handler<Data>() {
        @Override
        public void handle(Data event) {
            SortedMap<Key, byte[]> data = null;
            try {
                data = deserialise(event.data);
                trigger(new BatchWrite(data), store);
            } catch (IOException ex) {
                LOG.error("Could not deserialise data. Ignoring message!", ex);
            }
        }
    };
    Handler<Complete> completeHandler = new Handler<Complete>() {

        @Override
        public void handle(Complete event) {
            trigger(new Completed(id), transfer);
            state = State.DONE;
        }
    };
}
