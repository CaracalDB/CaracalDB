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
package se.sics.caracaldb.paxos;

import java.awt.Desktop;
import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.paxos.Commands.ChurnEvent;
import se.sics.caracaldb.paxos.Commands.Fail;
import se.sics.caracaldb.paxos.Commands.Join;
import se.sics.kompics.Kompics;
import se.sics.kompics.Scheduler;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.p2p.experiment.dsl.adaptor.Operation;
import se.sics.kompics.simulation.SimulatorScheduler;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
@RunWith(JUnit4.class)
public class PaxosTest {

    private static final Logger LOG = LoggerFactory.getLogger(PaxosTest.class);
    public static final int SEED = 0;
    private static final int BOOT_NUM = 3;
    private static final int OP_NUM = 500;
    private static final int CHURN_NUM = 4;
    
    private static SimulationScenario scenario;
    private static Scheduler scheduler;
    private static DecisionStore store;
    
    public static SimulationScenario getScenario() {
        return scenario;
    }
    
    public static Scheduler getScheduler() {
        return scheduler;
    }
    
    public static void setStore(DecisionStore store) {
        PaxosTest.store = store;
    }
    
    @Before
    public void setUp() {
        scenario = null;
        scheduler = null;
        store = null;
    }
    
    @Test
    public void basic() {
        scenario = new SimulationScenario() {
            {
                StochasticProcess bootProc = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opStart(BOOT_NUM));
                    }
                };
                
                StochasticProcess slowOpProc = new StochasticProcess() {
                    {
                        eventInterArrivalTime(uniform(200, 500));
                        raise(OP_NUM, opOp());
                    }
                };

                StochasticProcess verifyProc = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(Integer.MAX_VALUE, opVerify());
                    }
                };

                bootProc.start();
                //termProc.start();
                slowOpProc.startAfterTerminationOf(1000, bootProc);
                verifyProc.startAfterStartOf(OP_NUM*500, slowOpProc);
                terminateAfterTerminationOf(OP_NUM * 1000, slowOpProc);
            }
        };
        
        doTest("basic");
    }
    
    @Test
    public void fastOps() {
        scenario = new SimulationScenario() {
            {
                SimulationScenario.StochasticProcess bootProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opStart(BOOT_NUM));
                    }
                };
                
                SimulationScenario.StochasticProcess fastOpProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(uniform(50, 100));
                        raise(OP_NUM, opOp());
                    }
                };

                SimulationScenario.StochasticProcess verifyProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(Integer.MAX_VALUE, opVerify());
                    }
                };

                bootProc.start();
                //termProc.start();
                fastOpProc.startAfterTerminationOf(1000, bootProc);
                verifyProc.startAfterStartOf(OP_NUM*500, fastOpProc);
                terminateAfterTerminationOf(OP_NUM * 1000, fastOpProc);
            }
        };
        
        doTest("fastOps");
    }
    
    @Test
    public void singleFail() {
        scenario = new SimulationScenario() {
            {
                SimulationScenario.StochasticProcess bootProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opStart(BOOT_NUM));
                    }
                };
                
                StochasticProcess slowOpProc = new StochasticProcess() {
                    {
                        eventInterArrivalTime(uniform(200, 500));
                        raise(OP_NUM, opOp());
                    }
                };
                
                StochasticProcess nodeFailProc = new StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opFail());
                    }
                };

                SimulationScenario.StochasticProcess verifyProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(Integer.MAX_VALUE, opVerify());
                    }
                };

                bootProc.start();
                //termProc.start();
                slowOpProc.startAfterTerminationOf(1000, bootProc);
                nodeFailProc.startAfterStartOf(OP_NUM*200, slowOpProc);
                verifyProc.startAfterStartOf(OP_NUM*500, slowOpProc);
                terminateAfterTerminationOf(OP_NUM * 1000, slowOpProc);
            }
        };
        
        doTest("singleFail");
    }
    
    @Test
    public void churn() {
        scenario = new SimulationScenario() {
            {
                SimulationScenario.StochasticProcess bootProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(0));
                        raise(1, opStart(BOOT_NUM));
                    }
                };
                
                SimulationScenario.StochasticProcess slowOpProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(uniform(200, 500));
                        raise(OP_NUM, opOp());
                    }
                };
                
                SimulationScenario.StochasticProcess churnProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(uniform(20000, 30000));
                        raise(CHURN_NUM, opChurn());
                    }
                };

                SimulationScenario.StochasticProcess verifyProc = new SimulationScenario.StochasticProcess() {
                    {
                        eventInterArrivalTime(constant(500));
                        raise(Integer.MAX_VALUE, opVerify());
                    }
                };

                bootProc.start();
                slowOpProc.startAfterTerminationOf(1000, bootProc);
                churnProc.startAfterStartOf(OP_NUM*50, slowOpProc);
                verifyProc.startAfterStartOf(OP_NUM*500, slowOpProc);
                terminateAfterTerminationOf(OP_NUM * 1000, slowOpProc);
            }
        };
        
        doTest("churn");
    }
    
    private void doTest(String prefix) {
        scenario.setSeed(SEED);
        
        System.out.println("\n %%%%%%%%%%%%%%%%\n Executing Test: " + prefix + "\n%%%%%%%%%%%%%%%%\n");
        
        simulate();
        assertNotNull(store);
        assertEquals(OP_NUM, store.numOps());
        //printStore(prefix);
        store.validate();
    }
    
    private void printStore(String prefix) {
        String folderName = "/tmp/caracaldb/tests";
        File folder = new File(folderName);
        if (!folder.exists()) {
            folder.mkdirs();
        }
        String fileName = folderName+"/"+prefix+System.currentTimeMillis()+".html";
        File f = new File(fileName);
        try {
            f.createNewFile();
            StringBuilder sb = new StringBuilder();
            store.html(sb);
            FileUtils.writeStringToFile(f, sb.toString());
            Desktop.getDesktop().browse(f.toURI());
        } catch (IOException ex) {
            java.util.logging.Logger.getLogger(PaxosTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    private void simulate() {
        scheduler = new SimulatorScheduler();
        Kompics.setScheduler(scheduler);
        Kompics.createAndStart(SimulatorMain.class, 1);
    }
    
    public static Operation<Commands.Start> opStart(final int n) {
        return new Operation<Commands.Start>() {
            @Override
            public Commands.Start generate() {
                return new Commands.Start(n);
            }
        };
    }

    public static Operation<Commands.Verify> opVerify() {
        return new Operation<Commands.Verify>() {
            @Override
            public Commands.Verify generate() {
                return new Commands.Verify();
            }
        };
    }
    
    public static Operation<Commands.Operation> opOp() {
        return new Operation<Commands.Operation>() {

            @Override
            public Commands.Operation generate() {
                return new Commands.Operation();
            }
        };
    }
    
    public static Operation<Commands.Join> opJoin() {
        return new Operation<Commands.Join>() {

            @Override
            public Join generate() {
                return new Commands.Join();
            }
        };
    }
    
    public static Operation<Commands.Fail> opFail() {
        return new Operation<Commands.Fail>() {

            @Override
            public Fail generate() {
                return new Commands.Fail();
            }
        };
    }
    
    public static Operation<Commands.ChurnEvent> opChurn() {
        return new Operation<Commands.ChurnEvent>() {
            
            private int i = -1;

            @Override
            public ChurnEvent generate() {
                i++;
                if (i % 2 == 0) {
                    return new Fail();
                } else {
                    return new Join();
                }
            }
        };
    }
    
}
