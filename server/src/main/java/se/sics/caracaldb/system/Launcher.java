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
package se.sics.caracaldb.system;

import java.io.File;
import se.sics.caracaldb.persistence.DatabaseManager;
import se.sics.caracaldb.simulation.SimulationHelper;
import se.sics.caracaldb.simulation.SimulatorMain;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Kompics;
import se.sics.kompics.Scheduler;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.simulator.scheduler.BasicSimulationScheduler;

/**
 *
 * @author Lars Kroll <lkr@lars-kroll.com>
 */
public abstract class Launcher {

    static {
        ServerMessageRegistrator.register();
        //MessageRegistrator.register();
    }

    private static Configuration.Builder configBuilder;
    private static Configuration config;
    private static Scheduler scheduler;
    private static boolean simulation = false;
    private static SimulationScenario scenario;

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
        SimulationHelper.reset();
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
        DatabaseManager.preloadStores(config.core());
        Kompics.createAndStart(LauncherComponent.class, Runtime.getRuntime().availableProcessors(), 20); // Yes 20 is totally arbitrary
    }

    public static void stop() {
        Kompics.shutdown();
    }

    public static void simulate(SimulationScenario scenario) {
        config = configBuilder.finalise();
        cleanUp(config.getString("caracal.database.pathHead"));
        newSimulate(SimulatorMain.class, scenario);
    }
    
    public static void cleanUp(String dbPath) {
        File dbDir = new File(dbPath);
        if (dbDir.exists() && dbDir.isDirectory()) {
            if (!removeDirectory(dbDir)) {
                throw new RuntimeException("Unable to clean DB directory");
            }
        }
    }
    
    //TODO Alex - not symlink safe, replace with java implementation once we move to java 7+
    public static boolean removeDirectory(File dbDir) {
        if (dbDir == null) {
            return false;
        }
        if (!dbDir.exists()) {
            return false;
        }
        if (!dbDir.isDirectory()) {
            return false;
        }

        File[] childrenFiles = dbDir.listFiles();
        if (childrenFiles == null) {
            return false;
        }
        for (File file : childrenFiles) {
            if (file.isDirectory()) {
                if (!removeDirectory(file)) {
                    return false;
                }
            } else {
                if (!file.delete()) {
                    return false;
                }
            }
        }
        return dbDir.delete();
    }

    
    //added by Alex
    public static void newSimulate(Class<? extends ComponentDefinition> simulatorClass, SimulationScenario scenario) {
        simulation = true;
        scheduler = new BasicSimulationScheduler();
        Launcher.scenario = scenario;
        Kompics.setScheduler(scheduler);
        Kompics.createAndStart(simulatorClass, 1);
    }
}
