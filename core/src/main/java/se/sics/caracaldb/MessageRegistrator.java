/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb;

import se.sics.caracaldb.system.StartVNode;
import se.sics.kompics.network.grizzly.kryo.KryoMessage;

/**
 * Register all necessary messages here so they get done before Grizzly is loaded
 * 
 * @author Lars Kroll <lkroll@sics.se>
 */
public class MessageRegistrator {
    public static void register() {
        KryoMessage.register(StartVNode.class);
    }
}
