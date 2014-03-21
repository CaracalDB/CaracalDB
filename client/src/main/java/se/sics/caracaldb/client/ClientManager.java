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
package se.sics.caracaldb.client;

import com.google.common.primitives.Ints;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Init;
import se.sics.kompics.Kompics;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.VirtualNetworkChannel;
import se.sics.kompics.network.grizzly.ConstantQuotaAllocator;
import se.sics.kompics.network.grizzly.GrizzlyNetwork;
import se.sics.kompics.network.grizzly.GrizzlyNetworkInit;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ClientManager extends ComponentDefinition {

    private static ClientManager INSTANCE;
    // Components
    Component network;
    Component timer;
    VirtualNetworkChannel vnc;
    // Instance
    private final int messageBufferSizeMax;
    private final int messageBufferSize;
    private final int sampleSize;
    private static Config conf = null;
    private final Address bootstrapServer;
    private final Address self;
    private final SortedSet<Address> vNodes = new TreeSet<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public ClientManager() {
        if (INSTANCE == null) {
            INSTANCE = this; // will work in Oracle JDK...Not sure about other implementations
        } else {
            throw new RuntimeException("Don't start the ClientManager twice! You are doing it wrong -.-");
        }
        if (conf == null) {
            conf = ConfigFactory.load();
        }
        messageBufferSizeMax = conf.getInt("caracal.messageBufferSizeMax") * 1024;
        messageBufferSize = conf.getInt("caracal.messageBufferSize") * 1024;
        sampleSize = conf.getInt("bootstrap.sampleSize");
        String ipStr = conf.getString("bootstrap.address.hostname");
        String localHost = conf.getString("client.address.hostname");
        int bootPort = conf.getInt("bootstrap.address.port");
        int localPort = conf.getInt("client.address.port");
        InetAddress localIP = null;
        InetAddress bootIp = null;
        try {
            bootIp = InetAddress.getByName(ipStr);
            localIP = InetAddress.getByName(localHost);
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex.getMessage());
        }
        bootstrapServer = new Address(bootIp, bootPort, null);
        self = new Address(localIP, localPort, null);
        TimestampIdFactory.init(self);

        network = create(GrizzlyNetwork.class, new GrizzlyNetworkInit(self, 8, 0, 0,
                messageBufferSize, messageBufferSizeMax,
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                new ConstantQuotaAllocator(5)));
        timer = create(JavaTimer.class, Init.NONE);
        vnc = VirtualNetworkChannel.connect(network.getPositive(Network.class));

    }

    public static ClientManager getInstance() {
        if (INSTANCE == null) { // Not the nicest singleton solution but fine for this
            synchronized (ClientManager.class) {
                if (INSTANCE == null) {
                    //MessageRegistrator.register();
                    Kompics.createAndStart(ClientManager.class, Runtime.getRuntime().availableProcessors());
                }
            }
        }
        return INSTANCE;
    }

    public static BlockingClient newClient() {
        if (INSTANCE == null) { // Not the nicest singleton solution but fine for this
            synchronized (ClientManager.class) {
                if (INSTANCE == null) {
                    //MessageRegistrator.register();
                    Kompics.createAndStart(ClientManager.class, Runtime.getRuntime().availableProcessors());
                }
            }
        }
        return INSTANCE.addClient();
    }

    public static void setConfig(Config c) {
        synchronized (ClientManager.class) {
            conf = c;
        }
    }

    public BlockingClient addClient() {
        lock.writeLock().lock();
        try {
            Address adr = addVNode();
            BlockingQueue<CaracalResponse> q = new LinkedBlockingQueue<>();
            BlockingQueue<DMMessage.Resp> dataModelQ = new LinkedBlockingQueue<>();
            Component cw = create(ClientWorker.class, new ClientWorkerInit(q, dataModelQ, adr, bootstrapServer, sampleSize));
            vnc.addConnection(adr.getId(), cw.getNegative(Network.class));
            connect(timer.getPositive(Timer.class), cw.getNegative(Timer.class));
            trigger(Start.event, cw.control());
            ClientWorker worker = (ClientWorker) cw.getComponent();
            return new BlockingClient(q, dataModelQ, worker);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private Address addVNode() {
        lock.writeLock().lock();
        try {
            if (vNodes.isEmpty()) {
                Address adr = self.newVirtual(Ints.toByteArray(0));
                vNodes.add(adr);
                return adr;
            }
            Address lastNode = vNodes.last();
            int lastId = Ints.fromByteArray(lastNode.getId());
            int newId = lastId + 1;
            Address adr = self.newVirtual(Ints.toByteArray(newId));
            vNodes.add(adr);
            return adr;
        } finally {
            lock.writeLock().unlock();
        }
    }
}
