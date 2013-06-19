/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos;


/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Fault;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Kompics;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.simulation.SimulatorScheduler;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.virtual.networkmodel.UniformRandomModel;
import se.sics.kompics.virtual.simulator.VirtualSimulator;
import se.sics.kompics.virtual.simulator.VirtualSimulatorInit;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class SimulatorMain extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(SimulatorMain.class);
    SimulationScenario scenario;

    public SimulatorMain() {
        VirtualSimulator.setSimulationPortType(PaxosExperiment.class);
        scenario = PaxosTest.getScenario();

        // create
        Component simulator = create(VirtualSimulator.class,
                new VirtualSimulatorInit((SimulatorScheduler) PaxosTest.getScheduler(),
                scenario, new UniformRandomModel(5, 90)));
        Component simulationComponent = create(SimulatorComponent.class, Init.NONE);

        // connect
        connect(simulationComponent.getNegative(Network.class),
                simulator.getPositive(Network.class));
        connect(simulationComponent.getNegative(Timer.class),
                simulator.getPositive(Timer.class));
        connect(simulationComponent.getNegative(PaxosExperiment.class),
                simulator.getPositive(PaxosExperiment.class));
    }
    Handler<Fault> faultHandler = new Handler<Fault>() {
        @Override
        public void handle(Fault event) {
            LOG.error("Fault: {}", event.getFault());
            event.getFault().printStackTrace();
            Kompics.forceShutdown();
        }
    };
}
