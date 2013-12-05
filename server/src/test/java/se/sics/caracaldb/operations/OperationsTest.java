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
package se.sics.caracaldb.operations;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.simulation.SimulationGen;
import se.sics.caracaldb.simulation.SimulationHelper;
import se.sics.caracaldb.simulation.ValidationStore;
import se.sics.caracaldb.system.BootTest;
import se.sics.caracaldb.system.Launcher;
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

//    @Test
//    public void basic() {
//        SimulationHelper.type = SimulationHelper.ExpType.NO_RESULT;
//        SimulationScenario opScen = SimulationGen.putGet(BOOT_NUM, OP_NUM);
//        
//        Launcher.simulate(opScen);
//
//        ValidationStore store = SimulationHelper.getValidator();
//        assertNotNull(store);
//        store.print();
//        store.validate();
//    }
//    
//    @Test
//    public void rangeQueryTest() {
//        SimulationHelper.type = SimulationHelper.ExpType.WITH_RESULT;
//        SimulationScenario opScen = SimulationGen.rangeQuery(BOOT_NUM, 10*OP_NUM, OP_NUM/10);
//     
//        Launcher.simulate(opScen);
//        
//        assertNotNull(SimulationHelper.resultValidator);
//        assertTrue(SimulationHelper.resultValidator.experimentEnded());
//    }
    
    @Test
    public void test() {
        SimulationScenario testScen = SimulationGen.testScenario(BOOT_NUM);
        Launcher.newSimulate(testScen);
    }
}
