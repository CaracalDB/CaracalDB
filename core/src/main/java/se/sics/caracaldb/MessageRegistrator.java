/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb;

import se.sics.kompics.network.grizzly.kryo.KryoMessage;

/**
 * Register all necessary messages here so they get done before Grizzly is loaded
 * 
 * @author Lars Kroll <lkroll@sics.se>
 */
public class MessageRegistrator {
    public static void register() {
        KryoMessage.register(se.sics.caracaldb.system.StartVNode.class);
        
        KryoMessage.register(se.sics.caracaldb.operations.CaracalMsg.class);
        
        KryoMessage.register(se.sics.caracaldb.datatransfer.TransferMessage.class);
        
        KryoMessage.register(se.sics.caracaldb.global.ForwardMessage.class);
        KryoMessage.register(se.sics.caracaldb.global.SampleRequest.class);
        KryoMessage.register(se.sics.caracaldb.global.Sample.class);
        
        KryoMessage.register(se.sics.caracaldb.paxos.Paxos.PaxosMsg.class);
        
    }
}
