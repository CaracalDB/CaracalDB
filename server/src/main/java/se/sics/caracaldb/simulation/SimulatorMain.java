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
package se.sics.caracaldb.simulation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.simulation.command.Experiment;
import se.sics.caracaldb.system.Launcher;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Fault;
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
                scenario, new UniformRandomModel(1, 10)));
        Component simulationComponent = create(SimulatorComponent.class,
                new SimulatorComponentInit(Launcher.getConfig()));

        // connect
        connect(simulationComponent.getNegative(Network.class),
                simulator.getPositive(Network.class));
        connect(simulationComponent.getNegative(Timer.class),
                simulator.getPositive(Timer.class));
        connect(simulationComponent.getNegative(Experiment.class),
                simulator.getPositive(Experiment.class));
    }
    @Override
    public Fault.ResolveAction handleFault(Fault fault) {
        LOG.error(fault.toString());
        return Fault.ResolveAction.ESCALATE;
    }
}
