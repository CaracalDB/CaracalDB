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
package se.sics.caracaldb.simulation.main;

import se.sics.caracaldb.simulation.common.cmd.Cmd;
import se.sics.caracaldb.simulation.operations.datamodel.cmd.DMExp1Cmd;
import se.sics.caracaldb.simulation.operations.datamodel.cmd.DMTestCmd;
import se.sics.caracaldb.simulation.system.cmd.BootCmd;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class SimulationGen {

    private static int seed = 0;

    public static void setSeed(int i) {
        seed = i;
    }

    public static int getSeed() {
        return seed;
    }

    public static SimulationScenario testScenario(final int boot) {
        SimulationScenario scen = new SimulationScenario() {
            {
                SimulationScenario.StochasticProcess bootProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opBoot(boot));
                    }
                };

                SimulationScenario.StochasticProcess testProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(10, testCmd());
                    }
                };

                bootProc.start();
                testProc.startAfterTerminationOf(10000, bootProc);
                terminateAfterTerminationOf(20000, testProc);
            }
        };

        scen.setSeed(seed);

        return scen;
    }

    public static SimulationScenario expScenario(final int boot) {
        SimulationScenario scen = new SimulationScenario() {
            {
                SimulationScenario.StochasticProcess bootProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opBoot(boot));
                    }
                };

                SimulationScenario.StochasticProcess expProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(1, expCmd());
                    }
                };

                bootProc.start();
                expProc.startAfterTerminationOf(10000, bootProc);
                terminateAfterTerminationOf(20000, expProc);
            }
        };

        scen.setSeed(seed);
        return scen;
    }

    public static Operation<BootCmd> opBoot(final int n) {
        return new Operation<BootCmd>() {
            @Override
            public BootCmd generate() {
                return new BootCmd(n);
            }
        };
    }

    public static Operation<Cmd> testCmd() {
        return new Operation<Cmd>() {

            @Override
            public Cmd generate() {
                return new DMTestCmd();
            }

        };
    }
    
    public static Operation<Cmd> expCmd() {
        return new Operation<Cmd>() {

            @Override
            public Cmd generate() {
                return new DMExp1Cmd();
            }

        };
    }
}
