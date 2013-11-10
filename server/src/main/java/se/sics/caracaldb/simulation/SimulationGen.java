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

import se.sics.caracaldb.simulation.command.BootCmd;
import se.sics.caracaldb.simulation.command.GetCmd;
import se.sics.caracaldb.simulation.command.OpCmd;
import se.sics.caracaldb.simulation.command.PutCmd;
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
    
//    public static Operation<OpCmd> opOp() {
//        return new Operation<OpCmd>() {
//
//            @Override
//            public OpCmd generate() {
//                
//            }
//        };
//    }

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
