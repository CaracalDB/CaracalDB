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
package se.sics.caracaldb.simulation;

import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.simulation.command.BootCmd;
import se.sics.caracaldb.simulation.command.CustomRQCmd;
import se.sics.caracaldb.simulation.command.GetCmd;
import se.sics.caracaldb.simulation.command.PutCmd;
import se.sics.caracaldb.simulation.command.RandomRQCmd;
import se.sics.caracaldb.simulation.command.TerminateCmd;
import se.sics.caracaldb.simulation.command.ValidateCmd;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class SimulationGen {

    private static int seed = 0;

    public static void setSeed(int i) {
        seed = i;
    }

    public static int getSeed() {
        return seed;
    }

    public static SimulationScenario bootN(final int n) {
        SimulationScenario scen = new SimulationScenario() {
            {
                StochasticProcess bootProc = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opBoot(n));
                    }
                };

                StochasticProcess termProc = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(Integer.MAX_VALUE, opTerminate()); // can never send enough terminate commands ;)
                    }
                };

                bootProc.start();
                //termProc.start();
                termProc.startAfterTerminationOf(5000, bootProc);
                terminateAfterTerminationOf(33 * 1000, termProc);
            }
        };

        scen.setSeed(seed);

        return scen;
    }

    public static SimulationScenario putGet(final int boot, final int ops) {
        SimulationScenario scen = new SimulationScenario() {
            {
                StochasticProcess bootProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opBoot(boot));
                    }
                };

                StochasticProcess opProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(uniform(200, 500));
                        raise(ops, opPut());
                        raise(ops, opGet());
                    }
                };

                StochasticProcess validateProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(Integer.MAX_VALUE, opValidate()); // can never send enough terminate commands ;)
                    }
                };

                bootProc.start();
                //termProc.start();
                opProc.startAfterTerminationOf(10000, bootProc);
                validateProc.startAfterStartOf(60000, opProc);
                terminateAfterTerminationOf(ops * 500000, opProc);
            }
        };

        scen.setSeed(seed);

        return scen;
    }

    public static SimulationScenario rangeQuery(final int boot, final int putOps, final int rqOps) {
        SimulationScenario scen = new SimulationScenario() {
            {
                StochasticProcess bootProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opBoot(boot));
                    }
                };

                StochasticProcess opPutProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(putOps, opPut());
                    }
                };

                StochasticProcess opRQProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500*Math.max(putOps/rqOps, 1)));
                        raise(rqOps, opRQ());
                    }
                };
                
                StochasticProcess opFullSchemaRQProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(1, opFullSchemaRQ());
                    }
                };

                StochasticProcess validateProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(Integer.MAX_VALUE, opValidate()); // can never send enough terminate commands ;)
                    }
                };

                bootProc.start();
                opPutProc.startAfterTerminationOf(10000, bootProc);
                opRQProc.startAfterTerminationOf(11200, bootProc);
                opFullSchemaRQProc.startAfterTerminationOf(10000, opPutProc);
                validateProc.startAfterTerminationOf((Math.max(putOps, rqOps) + 1) * 500 + 60000, opFullSchemaRQProc);
                terminateAfterTerminationOf(50000, validateProc);
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

    public static Operation<PutCmd> opPut() {
        return new Operation<PutCmd>() {

            @Override
            public PutCmd generate() {
                return new PutCmd();
            }
        };
    }

    public static Operation<GetCmd> opGet() {
        return new Operation<GetCmd>() {

            @Override
            public GetCmd generate() {
                return new GetCmd();
            }
        };
    }

    public static Operation<RandomRQCmd> opRQ() {
        return new Operation<RandomRQCmd>() {

            @Override
            public RandomRQCmd generate() {
                return new RandomRQCmd();
            }
        };
    }
    
    public static Operation<CustomRQCmd> opFullSchemaRQ() {
        return new Operation<CustomRQCmd>() {

            @Override
            public CustomRQCmd generate() {
                return new CustomRQCmd(KeyRange.prefix(SimulationHelper.schemaPrefix));
            }
        };
    }

    public static Operation<TerminateCmd> opTerminate() {
        return new Operation<TerminateCmd>() {
            @Override
            public TerminateCmd generate() {
                return TerminateCmd.event;
            }
        };
    }

    public static Operation<ValidateCmd> opValidate() {
        return new Operation<ValidateCmd>() {
            @Override
            public ValidateCmd generate() {
                return new ValidateCmd();
            }
        };
    }
}
