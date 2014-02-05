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

import se.sics.caracaldb.simulation.SimulationHelper;
import se.sics.caracaldb.simulation.SimulatorMain;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Kompics;
import se.sics.kompics.Scheduler;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.simulation.SimulatorScheduler;

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
    
    //added by Alex
    public static void newSimulate(Class<? extends ComponentDefinition> simulatorClass, SimulationScenario scenario) {
        simulation = true;
        config = configBuilder.finalise();
        scheduler = new SimulatorScheduler();
        Launcher.scenario = scenario;
        Kompics.setScheduler(scheduler);
        Kompics.createAndStart(simulatorClass, 1);
    }
}
