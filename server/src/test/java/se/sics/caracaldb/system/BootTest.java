/* 
 * This file is part of the CaracalDB distributed storage system.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
import se.sics.caracaldb.simulation.SimulationHelper;
import se.sics.caracaldb.system.Configuration.NodePhase;
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
        SimulationHelper.type = SimulationHelper.ExpType.NO_RESULT;
        SimulationScenario bootScen = SimulationGen.bootN(BOOT_NUM);
        
        Launcher.config().addVirtualHook(NodePhase.INIT, new VirtualComponentHook(){

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
