/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Network;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class CatHerder extends ComponentDefinition {
    
    {
        final Negative<LookupService> lookup = provides(LookupService.class);
        final Positive<Network> net = requires(Network.class);
        
        //final Handler<
    }
}
