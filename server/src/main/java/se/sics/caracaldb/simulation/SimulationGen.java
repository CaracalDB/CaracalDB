/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
