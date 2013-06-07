/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.caracaldb.MessageRegistrator;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Init;
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
public class LauncherComponent extends ComponentDefinition {
    private Component network;
    private Component timer;
    private Component manager;
    VirtualNetworkChannel vnc;
    
    static {
        MessageRegistrator.register();
    }
    
    {
        Configuration config = Launcher.getCurrentConfig();
        Address netSelf = new Address(config.getIp(), config.getPort(), null);
        network = create(GrizzlyNetwork.class, new GrizzlyNetworkInit(netSelf, 8, 0, 0, 2 * 1024, 16 * 1024,
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                new ConstantQuotaAllocator(5)));
        timer = create(JavaTimer.class, Init.NONE);
        vnc = VirtualNetworkChannel.connect(network.getPositive(Network.class));
        manager = create(HostManager.class, new HostManagerInit(config, netSelf, vnc));
        
        connect(manager.getNegative(Timer.class), timer.getPositive(Timer.class));
        
    }
    
}
