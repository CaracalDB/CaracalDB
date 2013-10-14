/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import java.util.Arrays;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;
import se.sics.caracaldb.operations.RangeQuery;
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
    Negative<MaintenanceService> maintenance = provides(MaintenanceService.class);
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
        subscribe(forwardToRangeHandler, lookup);
        subscribe(bootedHandler, maintenance);
        subscribe(forwardMsgHandler, net);
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
            Message msg = event.msg.insertDestination(dest);
            trigger(msg, net);
            LOG.debug("{}: Forwarding {} to {}", new Object[]{self, event.msg, dest});
            //TODO rewrite to check at destination and foward until reached right node
        }
    };
    Handler<ForwardToRange> forwardToRangeHandler = new Handler<ForwardToRange>() {

        @Override
        public void handle(ForwardToRange event) {
            if (event.execType.equals(RangeQuery.Type.SEQUENTIAL)) {
                Pair<KeyRange, Address[]> repGroup;
                try {
                    repGroup = lut.getFirstResponsibles(event.range);
                    if (repGroup == null) {
                        LOG.warn("No responsible nodes for range");
                        return;
                    }
                } catch (LookupTable.BrokenLut ex) {
                    LOG.error("Broken lut");
                    System.exit(1);
                    return;
                }

                int nodePos = RAND.nextInt(repGroup.getValue1().length);
                Address dest = repGroup.getValue1()[nodePos];
                Message msg = event.getSubRangeMessage(repGroup.getValue0(), dest);
                trigger(msg, net);
                LOG.debug("{}: Forwarding {} to {}", new Object[]{self, msg, dest});
            } else { //if(event.execType.equals(RangeQuery.Type.Parallel)
                NavigableMap<KeyRange, Address[]> repGroups;
                try {
                    repGroups = lut.getAllResponsibles(event.range);
                    if (repGroups.isEmpty()) {
                        LOG.warn("No responsible nodes for range");
                        return;
                    }
                } catch (LookupTable.BrokenLut ex) {
                    LOG.error("Broken lut");
                    System.exit(1);
                    return;
                }
                for (Entry<KeyRange, Address[]> repGroup : repGroups.entrySet()) {
                    int nodePos = RAND.nextInt(repGroup.getValue().length);
                    Address dest = repGroup.getValue()[nodePos];
                    Message msg = event.getSubRangeMessage(repGroup.getKey(), dest);
                    trigger(msg, net);
                    LOG.debug("{}: Forwarding {} to {}", new Object[]{self, msg, dest});
                }
            }
        }
    };
    Handler<ForwardMessage> forwardMsgHandler = new Handler<ForwardMessage>() {
        @Override
        public void handle(ForwardMessage event) {
            Address[] repGroup = lut.getResponsibles(event.forwardTo);
            if (repGroup == null) {
                LOG.warn("No Node found reponsible for key {}! Dropping messsage.", event.forwardTo);
                return;
            }
            int nodePos = RAND.nextInt(repGroup.length);
            Address dest = repGroup[nodePos];
            Message msg = event.msg.insertDestination(dest);
            trigger(msg, net);
            LOG.debug("{}: Forwarding {} to {}", new Object[]{self, event.msg, dest});
        }
    };
    Handler<NodeBooted> bootedHandler = new Handler<NodeBooted>() {
        @Override
        public void handle(NodeBooted event) {
            Key nodeId = new Key(event.node.getId());
            View view = lut.getView(nodeId);
            KeyRange responsibility = lut.getResponsibility(nodeId);
            int quorum = view.members.size() / 2 + 1;
            NodeJoin join = new NodeJoin(view, quorum, responsibility, (view.id != 0));
            trigger(new MaintenanceMsg(self, event.node, join), net);
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
        LOG.debug("{}: Initial nodes are {}", self, localNodes);
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
