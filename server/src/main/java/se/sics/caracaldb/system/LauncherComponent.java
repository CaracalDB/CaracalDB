/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

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
    private Component deads;
    private Component timer;
    private Component manager;
    VirtualNetworkChannel vnc;
    
    static {
        ServerMessageRegistrator.register();
    }
    
    {
        Configuration config = Launcher.getConfig();
        Address netSelf = new Address(config.getIp(), config.getInt("server.address.port"), null);
        network = create(GrizzlyNetwork.class, new GrizzlyNetworkInit(netSelf, 8, 0, 0, 
                config.getInt("caracal.network.messageBufferSize"), 
                config.getInt("caracal.network.messageBufferSizeMax"),
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().availableProcessors(),
                new ConstantQuotaAllocator(5)));
        timer = create(JavaTimer.class, Init.NONE);
        deads = create(DeadLetterBox.class, new DeadLetterBoxInit(netSelf));
        vnc = VirtualNetworkChannel.connect(network.getPositive(Network.class), deads.getNegative(Network.class));
        manager = create(HostManager.class, new HostManagerInit(config, netSelf, vnc));
        
        connect(manager.getNegative(Timer.class), timer.getPositive(Timer.class));
        
    }
    
}
