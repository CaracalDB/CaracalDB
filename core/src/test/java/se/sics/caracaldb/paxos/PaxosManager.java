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
package se.sics.caracaldb.paxos;

import com.google.common.primitives.Ints;
import java.io.Serializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.View;
import se.sics.caracaldb.fd.EventualFailureDetector;
import se.sics.caracaldb.fd.SimpleEFD;
import se.sics.caracaldb.replication.log.Decide;
import se.sics.caracaldb.replication.log.Propose;
import se.sics.caracaldb.replication.log.Reconfigure;
import se.sics.caracaldb.replication.log.ReplicatedLog;
import se.sics.caracaldb.replication.log.Value;
import se.sics.kompics.Component;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class PaxosManager extends ComponentDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(PaxosManager.class);
    Positive<PaxosManagerPort> pm = requires(PaxosManagerPort.class);
    Positive<Network> net = requires(Network.class);
    Positive<Timer> timer = requires(Timer.class);
    Positive<EventualFailureDetector> fd = requires(EventualFailureDetector.class);
    Positive<ReplicatedLog> consensus = requires(ReplicatedLog.class);
    Component paxos;
    Component fdComp;
    private Address self;
    private DecisionStore store;
    private View view;

    public PaxosManager(PaxosManagerInit init) {

        self = init.self;
        store = init.store;
        view = init.view;

        int quorum = 0;
        if (view != null) {
            quorum = view.members.size() / 2 + 1;
        }
        fdComp = create(SimpleEFD.class, new SimpleEFD.Init(init.networkBound, self));
        paxos = create(Paxos.class, new PaxosInit(view, quorum, init.networkBound, self));

        connect(paxos.getPositive(ReplicatedLog.class), consensus.getPair());
        connect(fdComp.getPositive(EventualFailureDetector.class), fd.getPair());
        // neg
        connect(fdComp.getNegative(Network.class), net);
        connect(fdComp.getNegative(Timer.class), timer);
        connect(paxos.getNegative(Network.class), net);
        //connect(paxos.getNegative(Timer.class), timer);
        connect(paxos.getNegative(EventualFailureDetector.class), fd);

        // subscriptions
        subscribe(proposeHandler, pm);
        subscribe(decideHandler, consensus);
        subscribe(stopHandler, control);
    }
    Handler<Propose> proposeHandler = new Handler<Propose>() {
        @Override
        public void handle(Propose event) {
            LOG.debug("{}: Got Propose({})", self, event.value.id);
            trigger(event, consensus);
        }
    };
    Handler<Decide> decideHandler = new Handler<Decide>() {
        @Override
        public void handle(Decide event) {
            Value v = event.value;
            if (v instanceof PaxosOp) {
                LOG.debug("{}: Got Decide({}) in epoch {}", new Object[]{self, v.id, view.id});
                store.decided(view.id, self, v.id);
                return;
            }
            if (v instanceof Reconfigure) {
                Reconfigure rconf = (Reconfigure) v;
                if (view == null) {
                    store.joined(self);
                }
                view = rconf.view;
                LOG.debug("{}: Got Reconfigure, going to epoch {}", self, view.id);
            }
        }
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {
        @Override
        public void handle(Stop event) {
            LOG.debug("{}: Failed in epoch {}", self, view.id);
            store.fail(view.id, self);
        }
    };

    @Override
    public void tearDown() {
        disconnect(paxos.getPositive(ReplicatedLog.class), consensus.getPair());
        disconnect(fdComp.getPositive(EventualFailureDetector.class), fd.getPair());
        // neg
        disconnect(fdComp.getNegative(Network.class), net);
        disconnect(fdComp.getNegative(Timer.class), timer);
        disconnect(paxos.getNegative(Network.class), net);
        //connect(paxos.getNegative(Timer.class), timer);
        disconnect(paxos.getNegative(EventualFailureDetector.class), fd);

        destroy(fdComp);
        destroy(paxos);
    }

    public static class PaxosOp extends Value implements Serializable {

        public PaxosOp(long id) {
            super(id);
        }

        @Override
        public int compareTo(Value o) {
            if (o instanceof PaxosOp) {
                PaxosOp pop = (PaxosOp) o;
                return Ints.checkedCast(id - pop.id);
            }
            return super.baseCompareTo(o);
        }

        @Override
        public String toString() {
            return "PaxosOp(" + id + ")";
        }
    }
}
