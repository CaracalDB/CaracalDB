/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.vhostfd;

import se.sics.caracaldb.fd.SubscribeNodeStatus;
import se.sics.caracaldb.fd.Restore;
import se.sics.caracaldb.fd.Suspect;
import java.util.HashMap;
import java.util.UUID;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class HostProber {

    private UUID intervalPingTimeoutId;
    private UUID pongTimeoutId;
    private boolean suspected;
    private VirtualEPFD fd;
    //private Address firstRequest;
    private HostResponseTime times;
    private HashMap<UUID, SubscribeNodeStatus> requests = new HashMap<UUID, SubscribeNodeStatus>();
    private Address probedHost;

    HostProber(Address probedHost, VirtualEPFD fd) {
        this.probedHost = probedHost;
        this.fd = fd;

        suspected = false;

        this.times = new HostResponseTime(fd.minRto);
    }

    void start() {
        intervalPingTimeoutId = fd.setPingTimer(suspected, probedHost);
    }

    void ping() {
        intervalPingTimeoutId = fd.setPingTimer(suspected, probedHost);
        pongTimeoutId = fd.sendPing(System.nanoTime(), probedHost,
                times.getRTO());
//		logger.debug("@{}: PING {}", pongTimeoutId);
    }

    void pong(UUID pongId, long ts) {
        long RTT = System.nanoTime() - ts;
        times.updateRTO(RTT);

//		logger.debug("@{}: PoNG {} RTT={}", pongId, RTT);

        if (suspected == true) {
            suspected = false;
            reviseSuspicion();
        }
    }

    void pongTimedOut() {
        if (suspected == false) {
            suspected = true;
            times.timedOut();
            suspect();
        }
    }

    private void suspect() {
//		logger.debug("Peer {} is suspected", probedPeer);
        for (SubscribeNodeStatus req : requests.values()) {
            Address addr = req.node;
            Suspect sus = new Suspect(req, addr);
            fd.suspect(sus);
        }
    }

    private void reviseSuspicion() {
        for (SubscribeNodeStatus req : requests.values()) {
            Address addr = req.node;
            Restore res = new Restore(req, addr);
            fd.restore(res);
        }
    }

    void stop() {
        fd.stop(intervalPingTimeoutId, pongTimeoutId);
    }

    void addRequest(SubscribeNodeStatus request) {
        requests.put(request.requestId, request);
//        if (firstRequest == null) {
//            firstRequest = request.node;
//        }
    }

    boolean removeRequest(UUID requestId) {
        requests.remove(requestId);
        return requests.isEmpty();
    }

    boolean hasRequest(UUID requestId) {
        return requests.containsKey(requestId);
    }
}
