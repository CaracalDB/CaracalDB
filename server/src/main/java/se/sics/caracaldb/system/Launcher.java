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

    private static Configuration currentConfig;
    private static Scheduler scheduler;
    private static boolean simulation = false;
    private static SimulationScenario scenario;
    private static ValidationStore validator;

    public static void main(String[] args) {
        reset();
        Config conf = ConfigFactory.load();
        currentConfig.setMessageBufferSizeMax(conf.getInt("caracal.messageBufferSizeMax") * 1024);
        currentConfig.setMessageBufferSizeMax(conf.getInt("caracal.messageBufferSize") * 1024);
        currentConfig.setKeepAlivePeriod(conf.getLong("caracal.keepAlivePeriod"));
        currentConfig.setBootThreshold(conf.getInt("caracal.bootThreshold"));
        currentConfig.setBootVNodes(conf.getInt("caracal.bootVNodes"));
        String ipStr = conf.getString("bootstrap.address.hostname");
        String localHost = conf.getString("server.address.hostname");
        int bootPort = conf.getInt("bootstrap.address.port");
        int localPort = conf.getInt("server.address.port");
        InetAddress localIp = null;
        InetAddress bootIp = null;
        try {
            bootIp = InetAddress.getByName(ipStr);
            localIp = InetAddress.getByName(localHost);
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex.getMessage());
        }
        Address bootstrapServer = new Address(bootIp, bootPort, null);
        currentConfig.setBootstrapServer(bootstrapServer);
        currentConfig.setIp(localIp);
        currentConfig.setPort(localPort);
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
        currentConfig = new Configuration();
        simulation = false;
        scheduler = null;
        scenario = null;
        validator = null;
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
