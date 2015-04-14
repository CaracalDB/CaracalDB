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
package se.sics.caracaldb.vhostfd;

import java.util.HashMap;
import java.util.UUID;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.fd.Restore;
import se.sics.caracaldb.fd.SubscribeNodeStatus;
import se.sics.caracaldb.fd.Suspect;

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
