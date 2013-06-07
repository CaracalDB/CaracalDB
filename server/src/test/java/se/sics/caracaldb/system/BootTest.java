/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.caracaldb.TestUtil;
import se.sics.caracaldb.simulation.SimulationGen;
import se.sics.kompics.Component;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
@RunWith(JUnit4.class)
public class BootTest {
    
    
    @Before
    public void setUp() {

        Launcher.reset();

    }

    @Test
    public void basic() {
        SimulationScenario bootScen = SimulationGen.bootN(3);
        
        Configuration config = Launcher.getCurrentConfig();

        Launcher.simulate(bootScen);
    }
}
