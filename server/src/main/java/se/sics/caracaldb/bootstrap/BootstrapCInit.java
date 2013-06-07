/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.bootstrap;

import se.sics.caracaldb.system.Configuration;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class BootstrapCInit extends Init<BootstrapClient> {
    public final Address self;
    public final Configuration config;
    
    public BootstrapCInit(Address self, Configuration config) {
        this.self = self;
        this.config = config;
    }
}
