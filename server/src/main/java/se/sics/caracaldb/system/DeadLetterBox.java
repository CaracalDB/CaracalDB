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
package se.sics.caracaldb.system;

import java.util.HashSet;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.BaseMessage;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.global.ForwardMessage;
import se.sics.caracaldb.global.Forwardable;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.network.Msg;
import se.sics.kompics.network.Network;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DeadLetterBox extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DeadLetterBox.class);
    private static final int MAX_RECENTS = 10;

    Positive<Network> net = requires(Network.class);

    private final Address self;
    // Forward the same message only twice to avoid endless local loops
    private HashSet<UUID> recentForwardsFirstGen = new HashSet<UUID>();
    private HashSet<UUID> recentForwardsSecondGen = new HashSet<UUID>();

    public DeadLetterBox(DeadLetterBoxInit init) {
        self = init.self;
        subscribe(messageHandler, net);
    }

    Handler<Msg> messageHandler = new Handler<Msg>() {

        @Override
        public void handle(Msg event) {
            if (event instanceof BaseMessage) {
                BaseMessage msg = (BaseMessage) event;
                if (msg instanceof Forwardable) {
                    if (recentForwardsFirstGen.size() > MAX_RECENTS) {
                        recentForwardsFirstGen.clear();
                    }
                    if (recentForwardsSecondGen.size() > MAX_RECENTS) {
                        recentForwardsSecondGen.clear();
                    }
                    Forwardable f = (Forwardable) event;
                    if (recentForwardsSecondGen.contains(f.getId())) {
                        LOG.warn("{}: Not forwarding message anymore, due do possible endless loop: {}", self, event);
                        return;
                    }
                    if (recentForwardsFirstGen.contains(f.getId())) {
                        recentForwardsSecondGen.add(f.getId());
                    }
                    recentForwardsFirstGen.add(f.getId());
                    Key dest = new Key(msg.getHeader().getDstId());
                    trigger(new ForwardMessage(self, self, msg.getOrigin(), msg.getProtocol(), dest, f), net);
                    LOG.trace("{}: Forwarded message to LUTManager: {}", self, msg);
                    return;
                }
            }
            LOG.debug("{}: Dropping mis-addressed message: {}", self, event);
        }
    };
}
