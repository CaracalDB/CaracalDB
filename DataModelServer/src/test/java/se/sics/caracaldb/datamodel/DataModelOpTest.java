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
package se.sics.caracaldb.datamodel;

import org.junit.Before;
import org.junit.Test;
import se.sics.datamodel.system.DataModelLauncher;
import se.sics.datamodel.simulation.SimulationGen;
import se.sics.caracaldb.system.Launcher;
import se.sics.datamodel.simulation.DMSimulatorMain;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class DataModelOpTest {
    private static final int BOOT_NUM = 6;

    @Before
    public void setUp() {
        Launcher.reset();
    }

    @Test 
    public void testExp1() {
        DataModelLauncher.connectDataModel();
        SimulationScenario exp1Scen = SimulationGen.exp1Scenario(BOOT_NUM);
        Launcher.newSimulate(DMSimulatorMain.class, exp1Scen);
    }
}
