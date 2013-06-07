/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.caracaldb.simulation.SimulatorMain;
import se.sics.kompics.Kompics;
import se.sics.kompics.Scheduler;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.simulation.SimulatorScheduler;

/**
 *
 * @author Lars Kroll <lkr@lars-kroll.com>
 */
public abstract class Launcher {

    private static Configuration currentConfig;
    private static Scheduler scheduler;
    private static boolean simulation = false;
    private static SimulationScenario scenario;

    public static void main(String[] args) {
    }

    public static void reset() {
        if (Kompics.isOn()) {
            Kompics.shutdown();
        }
        currentConfig = new Configuration();
    }

    public static Configuration getCurrentConfig() {
        if (currentConfig == null) {
            reset();
        }
        return currentConfig;

    }

    public static Scheduler getScheduler() {
        return scheduler;
    }
    
    public static boolean isSimulation() {
        return simulation;
    }
    
    public static SimulationScenario getScenario() {
        return scenario;
    }

    public static void start() {
        Kompics.createAndStart(LauncherComponent.class, Runtime.getRuntime().availableProcessors(), 20); // Yes 20 is totally arbitrary
    }

    public static void stop() {
        Kompics.shutdown();
    }

    public static void simulate(SimulationScenario scenario) {
        simulation = true;
        scheduler = new SimulatorScheduler();
        Launcher.scenario = scenario;
        Kompics.setScheduler(scheduler);
        Kompics.createAndStart(SimulatorMain.class, 1);
    }
}
