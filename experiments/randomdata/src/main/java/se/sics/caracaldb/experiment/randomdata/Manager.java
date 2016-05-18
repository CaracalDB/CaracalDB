/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.experiment.randomdata;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.UUID;
import se.sics.caracaldb.Address;
import se.sics.kompics.Channel;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.ComponentProxy;
import se.sics.kompics.Handler;
import se.sics.kompics.KompicsEvent;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;
import se.sics.kompics.network.Transport;
import se.sics.kompics.network.data.DataNetwork;
import se.sics.kompics.network.netty.NettyInit;
import se.sics.kompics.network.netty.NettyNetwork;
import se.sics.kompics.network.virtual.VirtualNetworkChannel;
import se.sics.kompics.timer.Timer;
import se.sics.kompics.timer.java.JavaTimer;

/**
 *
 * @author lkroll
 */
public class Manager extends ComponentDefinition {

    // Components
    private final Component netC;
    private final Component timeC;
    // Ports
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    // instance
    private final VirtualNetworkChannel vnc;
    private final Address self;
    private final Init init;

    public Manager(Init init) {

        this.init = init;

        self = config().getValue("experiment.self.address", Address.class);

        timeC = create(JavaTimer.class, Sender.Init.NONE);
        connect(timer.getPair(), timeC.getPositive(Timer.class));
        netC = create(DataNetwork.class, new DataNetwork.Init(new DataNetwork.NetHook() {

            @Override
            public Component setupNetwork(ComponentProxy proxy) {
                Component nettyC = create(NettyNetwork.class, new NettyInit(self));
                return nettyC;
            }

            @Override
            public void connectTimer(ComponentProxy proxy, Component c) {
                proxy.connect(timeC.getPositive(Timer.class), c.getNegative(Timer.class), Channel.TWO_WAY);
            }
        }));
        connect(net.getPair(), netC.getPositive(Network.class));
        vnc = VirtualNetworkChannel.connect(net, proxy);

        subscribe(startHandler, control);
        subscribe(receiverHandler, loopback);
        subscribe(senderHandler, loopback);
    }

    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            init.managerFuture.set(Manager.this);
        }
    };

    Handler<StartReceiver> receiverHandler = new Handler<StartReceiver>() {

        @Override
        public void handle(StartReceiver event) {
            Component receiver = create(Receiver.class, Init.NONE);
            Address receiverAddress = self.newVirtual((byte)1);
            vnc.addConnection(receiverAddress.getId(), receiver.getNegative(Network.class));
            trigger(Start.event, receiver.control());
        }
    };
    Handler<StartSender> senderHandler = new Handler<StartSender>() {

        @Override
        public void handle(StartSender event) {
            Component sender = create(Sender.class, new Sender.Init(event.target.newVirtual((byte)1), event.proto));
            event.promise.set(sender.id());
            byte[] id = RandomDataSerializer.uuid2Bytes(sender.id());
            Address senderAddress = self.newVirtual(id);
            vnc.addConnection(id, sender.getNegative(Network.class));
            trigger(Start.event, sender.control());
        }
    };

    public void startReceiver() {
        trigger(StartReceiver.event, onSelf);
    }

    public ListenableFuture<UUID> startSender(Address target, Transport proto) {
        SettableFuture<UUID> idF = SettableFuture.create();
        trigger(new StartSender(target, proto, idF), onSelf);
        return idF;
    }

    public static class StartSender implements KompicsEvent {

        public final Address target;
        public final Transport proto;
        SettableFuture<UUID> promise;

        StartSender(Address target, Transport proto, SettableFuture<UUID> promise) {
            this.target = target;
            this.proto = proto;
            this.promise = promise;
        }
    }

    public static class StartReceiver implements KompicsEvent {

        public static final StartReceiver event = new StartReceiver();

        private StartReceiver() {

        }
    }

    public static class Init extends se.sics.kompics.Init<Manager> {

        public final SettableFuture<Manager> managerFuture;

        public Init(SettableFuture<Manager> managerFuture) {
            this.managerFuture = managerFuture;
        }
    }

}
