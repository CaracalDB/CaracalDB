/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.system;

import se.sics.caracaldb.MessageRegistrator;
import se.sics.kompics.network.grizzly.kryo.KryoMessage;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ServerMessageRegistrator {
    
    public static void register() {
        MessageRegistrator.register();
        
        KryoMessage.register(se.sics.caracaldb.vhostfd.VirtualEPFD.Ping.class);
        KryoMessage.register(se.sics.caracaldb.vhostfd.VirtualEPFD.Pong.class);
    }
}
