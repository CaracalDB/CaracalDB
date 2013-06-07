/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.kompics.virtual.simulator;

import se.sics.kompics.ChannelFilter;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.virtual.networkmodel.HostAddress;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class MessageDestinationFilter extends ChannelFilter<Message, HostAddress> {
    
    public MessageDestinationFilter(Address self) {
        super(Message.class, new HostAddress(self), true);
    }
    
    public MessageDestinationFilter(HostAddress self) {
        super(Message.class, self, true);
    }

    @Override
    public HostAddress getValue(Message event) {
        return new HostAddress(event.getDestination());
    }
    
}
