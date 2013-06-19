/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.simulation.command.Experiment;
import se.sics.caracaldb.system.Launcher;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Fault;
import se.sics.kompics.Handler;
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
        VirtualSimulator.setSimulationPortType(Experiment.class);
        scenario = Launcher.getScenario();

        // create
        Component simulator = create(VirtualSimulator.class,
                new VirtualSimulatorInit((SimulatorScheduler) Launcher.getScheduler(),
                scenario, new UniformRandomModel(5, 90)));
        Component simulationComponent = create(SimulatorComponent.class,
                new SimulatorComponentInit(Launcher.getCurrentConfig()));

        // connect
        connect(simulationComponent.getNegative(Network.class),
                simulator.getPositive(Network.class));
        connect(simulationComponent.getNegative(Timer.class),
                simulator.getPositive(Timer.class));
        connect(simulationComponent.getNegative(Experiment.class),
                simulator.getPositive(Experiment.class));
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
