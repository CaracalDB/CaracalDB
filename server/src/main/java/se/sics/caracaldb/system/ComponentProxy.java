/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ControlPort;
import se.sics.kompics.Event;
import se.sics.kompics.Negative;
import se.sics.kompics.Port;
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface ComponentProxy {

    public <P extends PortType> void trigger(Event e, Port<P> p);

    public Component create(Class<? extends ComponentDefinition> definition);

    public void destroy(Component component);

    public <P extends PortType> Channel<P> connect(Positive<P> positive, Negative<P> negative);

    public <P extends PortType> Channel<P> connect(Negative<P> negative, Positive<P> positive);
    
    public <P extends PortType> void disconnect(Negative<P> negative,Positive<P> positive);
    
    public <P extends PortType> void disconnect(Positive<P> positive, Negative<P> negative);
    
    public Negative<ControlPort> getControlPort();
}
