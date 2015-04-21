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
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.MessageRegistrator;
import se.sics.caracaldb.global.LUTPart;
import se.sics.caracaldb.global.ReadOnlyLUT;
import se.sics.caracaldb.global.SampleRequest;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.system.ComponentProxy;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ControlPort;
import se.sics.kompics.Handler;
import se.sics.kompics.Init;
import se.sics.kompics.Kompics;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Negative;
import se.sics.kompics.Port;
import se.sics.kompics.PortType;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.network.virtual.VirtualNetworkChannel;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ClientManager extends ComponentDefinition {

    private static final Random RAND = new Random();
    private static final Logger LOG = LoggerFactory.getLogger(ClientManager.class);
    private static ClientManager INSTANCE;
    // Ports
    Positive<Network> net = requires(Network.class);
    // Components
    Component network;
    Component timer;
    VirtualNetworkChannel vnc;
    // Instance
    private final boolean useLUT;
    private final int sampleSize;
    private static Config conf = null;
    private final Address bootstrapServer;
    private final Address self;
    private final SortedSet<Address> vNodes = new TreeSet<Address>();
    private static final BlockingQueue<Boolean> startedQ = new LinkedBlockingQueue<Boolean>(1);
    private final ReadOnlyLUT lut;
    private final ReadWriteLock lutLock = new ReentrantReadWriteLock();

    static {
        MessageRegistrator.register();
    }

    private ComponentProxy proxy = new ComponentProxy() {
        @Override
        public <P extends PortType> void trigger(KompicsEvent e, Port<P> p) {
            ClientManager.this.trigger(e, p);
        }

        @Override
        public void destroy(Component component) {
            ClientManager.this.destroy(component);
        }

        @Override
        public <P extends PortType> Channel<P> connect(Positive<P> positive, Negative<P> negative) {
            return ClientManager.this.connect(positive, negative);
        }

        @Override
        public <P extends PortType> Channel<P> connect(Negative<P> negative, Positive<P> positive) {
            return ClientManager.this.connect(negative, positive);
        }

        @Override
        public <P extends PortType> void disconnect(Negative<P> negative, Positive<P> positive) {
            ClientManager.this.disconnect(negative, positive);
        }

        @Override
        public <P extends PortType> void disconnect(Positive<P> positive, Negative<P> negative) {
            ClientManager.this.disconnect(positive, negative);
        }

        @Override
        public Negative<ControlPort> getControlPort() {
            return ClientManager.this.control;
        }

        @Override
        public <T extends ComponentDefinition> Component create(Class<T> definition, Init<T> initEvent) {
            return ClientManager.this.create(definition, initEvent);
        }

        @Override
        public <T extends ComponentDefinition> Component create(Class<T> definition, Init.None initEvent) {
            return ClientManager.this.create(definition, initEvent);
        }

    };

    public ClientManager() {
        if (INSTANCE == null) {
            INSTANCE = this; // will work in Oracle JDK...Not sure about other implementations
        } else {
            throw new RuntimeException("Don't start the ClientManager twice! You are doing it wrong -.-");
        }
        if (conf == null) {
            conf = ConfigFactory.load();
        }
        sampleSize = conf.getInt("bootstrap.sampleSize");
        useLUT = conf.getBoolean("client.fetchLUT");
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

        lut = new ReadOnlyLUT(self, RAND);

        network = create(NettyNetwork.class, new NettyInit(self));
        timer = create(JavaTimer.class, Init.NONE);
        vnc = VirtualNetworkChannel.connect(network.getPositive(Network.class));
        vnc.addConnection(null, net.getPair());

        subscribe(startHandler, control);
        subscribe(partHandler, net);
        subscribe(responseHandler, net);
    }

    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            try {
                startedQ.put(true);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            LOG.debug("Starting ClientManager {} with LUT? {}", self, useLUT);
            SampleRequest req = new SampleRequest(self, bootstrapServer, sampleSize, true, useLUT, -1);
            trigger(req, net);
        }
    };
    Handler<LUTPart> partHandler = new Handler<LUTPart>() {

        @Override
        public void handle(LUTPart event) {
            LOG.trace("{}: Got LUTPart!", self);
            lutLock.writeLock().lock();
            try {
                lut.collect(event);
            } finally {
                lutLock.writeLock().unlock();
            }
        }
    };
    Handler<CaracalMsg> responseHandler = new Handler<CaracalMsg>() {
        @Override
        public void handle(CaracalMsg event) {
            LOG.debug("Handling Message {}", event);
            if (event.op instanceof CaracalResponse) {
                CaracalResponse resp = (CaracalResponse) event.op;
                lutLock.writeLock().lock();
                try {
                    if (lut.collect(resp)) { // Might be a piece of a LUTUpdate
                        while (lut.hasMessages()) {
                            trigger(lut.pollMessages(), net);
                        }
                        return;
                    }
                } finally {
                    lutLock.writeLock().unlock();
                }
                LOG.error("Sending requests to clients is doing it wrong! {}", event);
            }
        }

    };

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
                    try {
                        //MessageRegistrator.register();
                        Kompics.createAndStart(ClientManager.class, Runtime.getRuntime().availableProcessors());
                        startedQ.take();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
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
        synchronized (this) {
            Address adr = addVNode();
            BlockingQueue<CaracalResponse> q = new LinkedBlockingQueue<CaracalResponse>();
            Component cw = create(ClientWorker.class, new ClientWorkerInit(q, adr, bootstrapServer, sampleSize, lut, lutLock));
            vnc.addConnection(adr.getId(), cw.getNegative(Network.class));
            connect(timer.getPositive(Timer.class), cw.getNegative(Timer.class));
            trigger(Start.event, cw.control());
            ClientWorker worker = (ClientWorker) cw.getComponent();
            return new BlockingClient(q, worker);
        }
    }

    public void addCustomClient(ClientHook hook) {

        synchronized (this) {
            Address vadd = addVNode();
            ClientSharedComponents sharedComponents = new ClientSharedComponents(vadd.getId());
            sharedComponents.setNetwork(vnc);
            sharedComponents.setSelf(vadd);
            sharedComponents.setTimer(timer.getPositive(Timer.class));
            sharedComponents.setBootstrapServer(bootstrapServer);
            sharedComponents.setSampleSize(sampleSize);

            hook.setUp(sharedComponents, proxy);
        }
    }

    private Address addVNode() {

        synchronized (this) {
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
        }
    }
}
