/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ResponseReceiver extends ComponentDefinition {
    
    private static final Logger LOG = LoggerFactory.getLogger(ResponseReceiver.class);
    
    Positive<Network> net = requires(Network.class);
    
    // instance
    private ValidationStore store;
    
    public ResponseReceiver(ResponseReceiverInit init) {
        this.store = init.store;
        subscribe(responseHandler, net);
    }
    
    Handler<CaracalMsg> responseHandler = new Handler<CaracalMsg>() {

        @Override
        public void handle(CaracalMsg event) {
            if (event.op instanceof CaracalResponse) {
                CaracalResponse resp = (CaracalResponse) event.op;
                LOG.debug("Got {} from {}", resp, event.getSource());
                store.response(resp);
            } else {
                LOG.debug("Got an unexpected message {}.", event);
            }
        }
    };
}
