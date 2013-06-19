/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import java.util.concurrent.ConcurrentSkipListSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.simulation.SimulationGen;
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
@RunWith(JUnit4.class)
public class BootTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(BootTest.class);
    
    private static final int BOOT_NUM = 3;
    
    private static ConcurrentSkipListSet<Address> bootedSet = new ConcurrentSkipListSet<Address>();
    
    @Before
    public void setUp() {

        Launcher.reset();
        bootedSet.clear();

    }

    @Test
    public void basic() {
        SimulationScenario bootScen = SimulationGen.bootN(BOOT_NUM);
        
        Configuration config = Launcher.getCurrentConfig();
        
        config.addVirtualHook(new VirtualComponentHook(){

            @Override
            public void setUp(VirtualSharedComponents shared, ComponentProxy parent) {
                Component rep = parent.create(ReporterComponent.class, new ReporterInit(shared.getSelf()));
            }

            @Override
            public void tearDown(VirtualSharedComponents shared, ComponentProxy parent) {
                throw new UnsupportedOperationException("Not supported yet.");
            }
        });

        Launcher.simulate(bootScen);
        
        for (Address adr : bootedSet) {
            System.out.println(adr);
        }
        
        assertTrue(BOOT_NUM <= bootedSet.size());
    }
    
    public static class ReporterComponent extends ComponentDefinition {
        
        private Address self;
        
        public ReporterComponent(ReporterInit init) {
            
            self = init.self;
            
            subscribe(startHandler, control);
            
            LOG.debug("{} initialized", self);
            
        }
        
        Handler<Start> startHandler = new Handler<Start>() {

            @Override
            public void handle(Start event) {
                LOG.debug("{} started", self);
                bootedSet.add(self);
            }
        };
    }
    
    public static class ReporterInit extends Init<ReporterComponent> {
        public final Address self;
        
        public ReporterInit(Address self) {
            this.self = self;
        }
    }
    
}
