/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.operations;

import java.util.concurrent.ConcurrentSkipListSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.simulation.SimulationGen;
import se.sics.caracaldb.simulation.ValidationStore;
import se.sics.caracaldb.system.BootTest;
import se.sics.caracaldb.system.ComponentProxy;
import se.sics.caracaldb.system.Configuration;
import se.sics.caracaldb.system.Launcher;
import se.sics.caracaldb.system.VirtualComponentHook;
import se.sics.caracaldb.system.VirtualSharedComponents;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class OperationsTest {

    private static final Logger LOG = LoggerFactory.getLogger(BootTest.class);
    private static final int BOOT_NUM = 6;
    private static final int OP_NUM = 200;

    @Before
    public void setUp() {

        Launcher.reset();

    }

    @Test
    public void basic() {
        SimulationScenario opScen = SimulationGen.putGet(BOOT_NUM, OP_NUM);


        Launcher.simulate(opScen);

        ValidationStore store = Launcher.getValidator();
        assertNotNull(store);
        store.print();
        store.validate();
    }
}
