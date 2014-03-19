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
package se.sics.datamodel.system;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.datamodel.DataModel;
import se.sics.datamodel.DataModelInit;
import se.sics.datamodel.DataModelPort;
import se.sics.datamodel.ServerInterface;
import se.sics.datamodel.ServerInterfaceInit;
import se.sics.caracaldb.system.ComponentHook;
import se.sics.caracaldb.system.ComponentProxy;
import se.sics.caracaldb.system.Configuration.SystemPhase;
import se.sics.caracaldb.system.HostSharedComponents;
import se.sics.caracaldb.system.Launcher;
import se.sics.kompics.Component;
import se.sics.kompics.Kompics;
import se.sics.kompics.Start;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DataModelLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(DataModelLauncher.class);

    public static void main(String[] args) {
        Launcher.reset();
        connectDataModel();
        Launcher.start();
        try {
            Kompics.waitForTermination();
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    public static void connectDataModel() {
        Launcher.config().addHostHook(SystemPhase.BOOTSTRAPPED, new ComponentHook() {
            @Override
            public void setUp(HostSharedComponents shared, ComponentProxy parent) {
                LOG.info("Setting up data model on {}", shared.getSelf());
                Component serverInterface = parent.create(ServerInterface.class, new ServerInterfaceInit(shared.getSelf()));
                Component dataModel = parent.create(DataModel.class, new DataModelInit());
                shared.connectNetwork(serverInterface);
                parent.connect(serverInterface.getNegative(DataModelPort.class), dataModel.getPositive(DataModelPort.class));
                parent.trigger(Start.event, serverInterface.control());
                parent.trigger(Start.event, dataModel.control());
            }

            @Override
            public void tearDown(HostSharedComponents shared, ComponentProxy parent) {
                LOG.info("Tearing down.");
            }
        });
    }
}
