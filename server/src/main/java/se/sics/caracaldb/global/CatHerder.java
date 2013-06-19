/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.system.StartVNode;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;
import se.sics.kompics.network.Network;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class CatHerder extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(CatHerder.class);
    private static final Random RAND = new Random();
    
    // ports
    Negative<LookupService> lookup = provides(LookupService.class);
    Positive<Network> net = requires(Network.class);
    
    // instance
    private LookupTable lut;
    private Address self;
    private boolean master;

    public CatHerder(CatHerderInit init) {
        lut = init.bootEvent.lut;
        self = init.self;
        if (checkMaster()) {
            connectMasterHandlers();
        } else {
            connectSlaveHandlers();
        }
        subscribe(startHandler, control);
        subscribe(lookupRHandler, lookup);
        subscribe(forwardHandler, lookup);
    }
    
    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.debug("{} starting initial nodes", self);
            startInitialVNodes();
        }
    };
    
    Handler<LookupRequest> lookupRHandler = new Handler<LookupRequest>() {

        @Override
        public void handle(LookupRequest event) {
            Address[] repGroup = lut.getResponsibles(event.key);
            LookupResponse rsp;
            if (repGroup == null) {
                LOG.warn("No Node found reponsible for key {}!", event.key);
                rsp = new LookupResponse(event, event.key, event.reqId, null);
            } else {
                rsp = new LookupResponse(event, event.key, event.reqId, Arrays.asList(repGroup));
            }
            trigger(rsp, lookup);
            
        }
    };
    
    Handler<ForwardToAny> forwardHandler = new Handler<ForwardToAny>() {

        @Override
        public void handle(ForwardToAny event) {
            Address[] repGroup = lut.getResponsibles(event.key);
            if (repGroup == null) {
                LOG.warn("No Node found reponsible for key {}! Dropping messsage.", event.key);
                return;
            }
            int nodePos = RAND.nextInt(repGroup.length);
            Address dest = repGroup[nodePos];
            Message msg = event.insertDestination(dest);
            trigger(msg, net);
            //TODO rewrite to check at destination and foward until reached right node
        }
    };
    
    private void connectMasterHandlers() {
        
    }
    
    private void connectSlaveHandlers() {
        
    }
    
    private void startInitialVNodes() {
        Set<Key> localNodes = lut.getVirtualNodesAt(self);
        for (Key k : localNodes) {
            trigger(new StartVNode(self, self, k.getArray()), net);
        }
    }
    
    private boolean checkMaster() {
        Address[] masterGroup = lut.getHosts(0);
        for (Address adr : masterGroup) {
            if (adr.equals(self)) {
                master = true;
                return master;
            }
        }
        master = false;
        return master;
    }
    
    
}
