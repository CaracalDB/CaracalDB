/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.simulation;

import se.sics.caracaldb.system.Configuration;
import se.sics.kompics.Init;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class SimulatorComponentInit extends Init<SimulatorComponent> {
    
    public final Configuration config;
    
    public SimulatorComponentInit(Configuration config) {
        this.config = config;
    }
    
}
