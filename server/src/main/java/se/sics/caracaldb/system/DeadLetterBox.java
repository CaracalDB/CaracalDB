/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.global.ForwardMessage;
import se.sics.caracaldb.global.Forwardable;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DeadLetterBox extends ComponentDefinition {
    
    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterBox.class);
    
    Positive<Network> net = requires(Network.class);
    
    private final Address self;
    
    public DeadLetterBox(DeadLetterBoxInit init) {
        self = init.self;
        subscribe(messageHandler, net);
    }
    
    Handler<Message> messageHandler = new Handler<Message>() {

        @Override
        public void handle(Message event) {
            if (event instanceof Forwardable) {
                Forwardable f = (Forwardable) event;
                Key dest = new Key(event.getSource().getId());
                trigger(new ForwardMessage(self, self, dest, f), net);
                return;
            }
            LOG.debug("{}: Dropping mis-addressed message: {}", self, event);
        }
    };
}
