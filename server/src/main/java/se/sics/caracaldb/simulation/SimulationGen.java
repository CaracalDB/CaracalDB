/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.simulation;

import se.sics.caracaldb.simulation.command.BootCmd;
import se.sics.caracaldb.simulation.command.TerminateCmd;
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

                StochasticProcess termProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(Integer.MAX_VALUE, opTerminate()); // can never send enough terminate commands ;)
                    }
                };

                bootProc.start();
                termProc.startAfterTerminationOf(5000, bootProc);
                terminateAfterTerminationOf(300 * 1000, termProc);
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

    public static Operation<TerminateCmd> opTerminate() {
        return new Operation<TerminateCmd>() {
            @Override
            public TerminateCmd generate() {
                return TerminateCmd.event;
            }
        };
    }
}
