/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import se.sics.caracaldb.simulation.SimulatorMain;
import se.sics.caracaldb.simulation.ValidationStore;
import se.sics.kompics.Kompics;
import se.sics.kompics.Scheduler;
import se.sics.kompics.address.Address;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.simulation.SimulatorScheduler;

/**
 *
 * @author Lars Kroll <lkr@lars-kroll.com>
 */
public abstract class Launcher {

    private static Configuration.Builder configBuilder;
    private static Configuration config;
    private static Scheduler scheduler;
    private static boolean simulation = false;
    private static SimulationScenario scenario;
    private static ValidationStore validator;

    public static void main(String[] args) {
        reset();
        start();
        try {
            Kompics.waitForTermination();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    public static void reset() {
        if (Kompics.isOn()) {
            Kompics.shutdown();
        }
        configBuilder = Configuration.Factory.load();
        config = null;
        simulation = false;
        scheduler = null;
        scenario = null;
        validator = null;
    }

    public static Configuration getConfig() {
        if (config == null) {
            throw new RuntimeException("Don't access the config before starting!");
        }
        return config;
    }
    
    public static Configuration.Builder config() {
        return configBuilder;
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
        config = configBuilder.finalise();
        Kompics.createAndStart(LauncherComponent.class, Runtime.getRuntime().availableProcessors(), 20); // Yes 20 is totally arbitrary
    }

    public static void stop() {
        Kompics.shutdown();
    }

    public static void simulate(SimulationScenario scenario) {
        simulation = true;
        config = configBuilder.finalise();
        scheduler = new SimulatorScheduler();
        Launcher.scenario = scenario;
        Kompics.setScheduler(scheduler);
        Kompics.createAndStart(SimulatorMain.class, 1);
    }

    /**
     * @return the validator
     */
    public static ValidationStore getValidator() {
        return validator;
    }

    /**
     * @param aValidator the validator to set
     */
    public static void setValidator(ValidationStore aValidator) {
        validator = aValidator;
    }
}
