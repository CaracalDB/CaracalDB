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
package se.sics.caracaldb.simulation.main;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.simulation.operations.OperationsPort;
import se.sics.caracaldb.simulation.operations.datamodel.DMOp1Component;
import se.sics.caracaldb.simulation.operations.datamodel.DMOp1Init;
import se.sics.caracaldb.simulation.system.SystemComponent;
import se.sics.caracaldb.simulation.system.SystemComponentInit;
import se.sics.caracaldb.simulation.system.SystemPort;
import se.sics.caracaldb.system.Launcher;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Fault;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Kompics;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.p2p.experiment.dsl.SimulationScenario;
import se.sics.kompics.simulation.SimulatorScheduler;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.virtual.networkmodel.UniformRandomModel;
import se.sics.kompics.virtual.simulator.VirtualSimulator;
import se.sics.kompics.virtual.simulator.VirtualSimulatorInit;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class NewSimulatorMain extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(NewSimulatorMain.class);
    SimulationScenario scenario;

    public NewSimulatorMain() {
        InetAddress localIP;
        try {
            localIP = InetAddress.getLocalHost();
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex.getMessage());
        }

        Address localAddr;
        localAddr = new Address(localIP, 40000, null);
        TimestampIdFactory.init(localAddr);

        int randSeed = 1234;

        VirtualSimulator.setSimulationPortType(SimulationPort.class);
        scenario = Launcher.getScenario();

        //simulator
        Component simulator = create(VirtualSimulator.class,
                new VirtualSimulatorInit((SimulatorScheduler) Launcher.getScheduler(),
                        scenario, new UniformRandomModel(1, 10)));
        Component simulation = create(SimulationComponent.class, Init.NONE);

        //simulation components
        Component system = create(SystemComponent.class, new SystemComponentInit(Launcher.getConfig()));
        Component operations = create(DMOp1Component.class, new DMOp1Init(localAddr, randSeed));

        //connect
        connect(simulation.getNegative(SimulationPort.class), simulator.getPositive(SimulationPort.class));
        connect(simulation.getNegative(SystemPort.class), system.getPositive(SystemPort.class));
        connect(simulation.getNegative(OperationsPort.class), operations.getPositive(OperationsPort.class));
        
        connect(system.getNegative(Network.class), simulator.getPositive(Network.class));
        connect(system.getNegative(Timer.class), simulator.getPositive(Timer.class));
        
        connect(operations.getNegative(Network.class), simulator.getPositive(Network.class));
        
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
