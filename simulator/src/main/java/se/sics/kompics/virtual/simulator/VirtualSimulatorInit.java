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
package se.sics.kompics.virtual.simulator;

import se.sics.kompics.Init;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.simulator.scheduler.SimulationScheduler;
import se.sics.kompics.virtual.networkmodel.NetworkModel;

/**
 *
 * The <code>P2pSimulatorInit</code> class.
 *
 *
 *
 * @author Cosmin Arad {@literal <cosmin@sics.se>}
 *
 * @version $Id: P2pSimulatorInit.java 1102 2009-08-31 13:23:16Z Cosmin $
 *
 */
public final class VirtualSimulatorInit extends Init<VirtualSimulator> {

    private final SimulationScheduler scheduler;

    private final SimulationScenario scenario;

    private final NetworkModel networkModel;

    public final boolean serialize;

    public VirtualSimulatorInit(SimulationScheduler scheduler,
            SimulationScenario scenario, NetworkModel networkModel) {

        super();

        this.scheduler = scheduler;

        this.scenario = scenario;

        this.networkModel = networkModel;

        serialize = true;

    }

    public VirtualSimulatorInit(SimulationScheduler scheduler,
            SimulationScenario scenario, NetworkModel networkModel, boolean serialize) {

        super();

        this.scheduler = scheduler;

        this.scenario = scenario;

        this.networkModel = networkModel;

        this.serialize = serialize;

    }

    public final SimulationScheduler getScheduler() {

        return scheduler;

    }

    public final SimulationScenario getScenario() {

        return scenario;

    }

    public final NetworkModel getNetworkModel() {

        return networkModel;

    }

}
