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
package se.sics.datamodel.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import se.sics.caracaldb.client.ClientHook;
import se.sics.caracaldb.client.ClientSharedComponents;
import se.sics.caracaldb.system.ComponentProxy;
import se.sics.datamodel.DMCoreMessageRegistrator;
import se.sics.datamodel.msg.DMMessage;
import se.sics.kompics.Component;
import se.sics.kompics.Start;
import se.sics.kompics.timer.Timer;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ClientManager {

    private static ClientManager INSTANCE;
    private static se.sics.caracaldb.client.ClientManager MANAGER;

    static {
        DMCoreMessageRegistrator.register();
    }
    
    public ClientManager() {
        MANAGER = se.sics.caracaldb.client.ClientManager.getInstance();
    }

    public static ClientManager getInstance() {
        if (INSTANCE == null) { // Not the nicest singleton solution but fine for this
            synchronized (ClientManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ClientManager();
                }
            }
        }
        return INSTANCE;
    }

    public static BlockingClient newClient() {
        return getInstance().addClient();
    }

    public BlockingClient addClient() {
        WorkerClientHook hook = new WorkerClientHook();
        MANAGER.addCustomClient(hook);
        return hook.client;
    }

    public class WorkerClientHook implements ClientHook {

        BlockingClient client;

        @Override
        public void setUp(ClientSharedComponents shared, ComponentProxy parent) {
            BlockingQueue<DMMessage.Resp> dataModelQ = new LinkedBlockingQueue<DMMessage.Resp>();
            Component cw = parent.create(ClientWorker.class, new ClientWorkerInit(dataModelQ, shared.getSelf(), shared.getBootstrapServer(), shared.getSampleSize()));
            shared.connectNetwork(cw);
            parent.connect(shared.getTimer(), cw.getNegative(Timer.class));
            parent.trigger(Start.event, cw.control());
            ClientWorker worker = (ClientWorker) cw.getComponent();
            client = new BlockingClient(dataModelQ, worker);
        }

        @Override
        public void tearDown(ClientSharedComponents shared, ComponentProxy parent) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }
}