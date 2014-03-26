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
package se.sics.datamodel.simulation;

import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.datamodel.simulation.cmd.Cmd;
import se.sics.datamodel.simulation.cmd.OperationsCmd;
import se.sics.datamodel.simulation.cmd.SystemCmd;
import se.sics.datamodel.simulation.cmd.common.msg.ConnectNode;
import se.sics.datamodel.simulation.cmd.common.msg.TerminateMsg;
import se.sics.datamodel.simulation.system.SystemPort;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Kompics;
import se.sics.kompics.Positive;
import se.sics.kompics.p2p.experiment.dsl.events.TerminateExperiment;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class SimulationComponent extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(SimulationComponent.class);

    private Positive<SimulationPort> simulator = requires(SimulationPort.class);
    private Positive<SystemPort> system = requires(SystemPort.class);
    private Positive<OperationsPort> operations = requires(OperationsPort.class);

    private boolean terminated = false;
    private int workingPorts = 2;

    public SimulationComponent() {
        subscribe(commandHadler, simulator);
        subscribe(killHandler, simulator);
        subscribe(terminateHandler, system);
        subscribe(terminateHandler, operations);
        subscribe(connectNodeHandler, system);
    }

    Handler<Cmd> commandHadler = new Handler<Cmd>() {

        @Override
        public void handle(Cmd cmd) {
            if(terminated) {
                return;
            }
            
            LOG.debug("{}", cmd);
            if (cmd instanceof SystemCmd) {
                trigger(cmd, system);
            } else if (cmd instanceof OperationsCmd) {
                trigger(cmd, operations);
            } else {
                LOG.warn("unknown command {}", cmd);
            }
        }

    };

    Handler<TerminateExperiment> killHandler = new Handler<TerminateExperiment>() {

        @Override
        public void handle(TerminateExperiment terminate) {
            LOG.info("killing experiment...");
            Kompics.forceShutdown();
        }

    };

    Handler<TerminateMsg.Ind> terminateHandler = new Handler<TerminateMsg.Ind>() {

        @Override
        public void handle(TerminateMsg.Ind event) {
            if(!terminated) {
                LOG.info("terminating...");
                terminated = true;
                trigger(new TerminateMsg.Req(), system);
                trigger(new TerminateMsg.Req(), operations);
            }
            workingPorts--;
            if (terminated && workingPorts == 0) {
                LOG.info("terminated");
                Kompics.forceShutdown();
            }
        }

    };

    Handler<ConnectNode.Ind> connectNodeHandler = new Handler<ConnectNode.Ind>() {

        @Override
        public void handle(ConnectNode.Ind event) {
            trigger(event, operations);
        }
        
    };
}
