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
package se.sics.caracaldb.bootstrap;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.global.LookupTable;
import se.sics.caracaldb.system.Configuration;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class BootstrapServer extends ComponentDefinition {

    private static enum State {

        COLLECTING, SEEDING, DONE;
    }
    // static
    private static final Logger log = LoggerFactory.getLogger(BootstrapServer.class);
    // ports
    private final Negative<Bootstrap> bootstrap = provides(Bootstrap.class);
    private final Positive<Timer> timer = requires(Timer.class);
    private final Positive<Network> net = requires(Network.class);
    // instance
    private Address self;
    private Configuration config;
    private Set<Address> fresh = new HashSet<Address>();
    private Set<Address> active = new HashSet<Address>();
    private State state;
    private ImmutableSet<Address> bootSet;
    private ImmutableSet<Address> waitSet;
    private Set<Address> readySet;
    private LookupTable lut;
    private byte[] lutData;

    private UUID timeoutId;

    public BootstrapServer(BootstrapSInit init) {
        // INIT
        self = init.self;
        config = init.config;
        state = State.COLLECTING;
        // Subscriptions
        subscribe(startHandler, control);
        subscribe(brHandler, net);
        subscribe(readyHandler, net);
        subscribe(timeoutHandler, timer);
    }
    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.info("Starting up bootstrap server on {}, waiting for {} nodes",
                    self, config.getInt("caracal.bootThreshold"));

            long keepAliveTimeout = config.getMilliseconds("caracal.network.keepAlivePeriod") * 2;
            SchedulePeriodicTimeout spt
                    = new SchedulePeriodicTimeout(keepAliveTimeout, keepAliveTimeout);
            spt.setTimeoutEvent(new ClearTimeout(spt));
            trigger(spt, timer);
            active.add(self);
        }
    };
    Handler<BootstrapRequest> brHandler = new Handler<BootstrapRequest>() {

        @Override
        public void handle(BootstrapRequest e) {
            if (state != State.DONE) {
                fresh.add(e.getSource());
                if (!active.contains(e.getSource())) {
                    log.debug("Got BootstrapRequest from {}", e.getSource());
                }
            }
            if (state == State.SEEDING) {
                trigger(new BootstrapResponse(self, e.getSource(), lutData), net);
            }
        }
    };
    Handler<Ready> readyHandler = new Handler<Ready>() {

        @Override
        public void handle(Ready e) {
            fresh.add(e.getSource());
            if (state == State.SEEDING) {
                readySet.add(e.getSource());
            }
        }
    };
    Handler<ClearTimeout> timeoutHandler = new Handler<ClearTimeout>() {

        @Override
        public void handle(ClearTimeout e) {
            if (timeoutId == null) {
                timeoutId = e.getTimeoutId();
            }

            if (state == State.COLLECTING) {
                active.clear();
                active.addAll(fresh);
                fresh.clear();
                active.add(self);
                log.debug("Cleaning up. {} hosts in the active set.", active.size());
                if (active.size() >= config.getInt("caracal.bootThreshold")) {
                    bootUp();
                }
            } else if (state == State.SEEDING) {
                active.clear();
                active.addAll(fresh);
                fresh.clear();
                active.add(self);
                // wait for all hosts that the LUT was generated with
                // except for those that failed and didn't come back
                SetView<Address> activeBoot = Sets.intersection(bootSet, active);
                waitSet = activeBoot.immutableCopy();
                readySet.retainAll(active);
                if (Sets.difference(waitSet, readySet).isEmpty()) {
                    startUp();
                }
            }
        }
    };

    private void bootUp() {
        log.info("Threshold reached. Seeding LUT.");
        bootSet = ImmutableSet.copyOf(active);
        waitSet = ImmutableSet.copyOf(active);
        Key vnodePrefix = Key.fromHex(config.getString("caracal.vnodePrefix"));
        lut = LookupTable.generateInitial(bootSet, config.getInt("caracal.bootVNodes"), vnodePrefix);
        StringBuilder sb = new StringBuilder();
        lut.printFormat(sb);
        System.out.println(sb.toString());
        lutData = lut.serialise();

        for (Address adr : bootSet) {
            if (!adr.equals(self)) {
                trigger(new BootstrapResponse(self, adr, lutData), net);
            }
        }
        state = State.SEEDING;
        readySet = new HashSet<Address>();
        readySet.add(self);
    }

    private void startUp() {
        log.info("Everyone of consequence is ready. Booting Up.");
        for (Address adr : active) {
            if (!adr.equals(self)) {

                trigger(new BootUp(self, adr), net);
            }
        }
        state = State.DONE;
        SetView<Address> failed = Sets.difference(bootSet, readySet);
        SetView<Address> joined = Sets.difference(active, bootSet);
        trigger(new Bootstrapped(lut, failed.immutableCopy(), joined.immutableCopy()), bootstrap);
        trigger(new CancelPeriodicTimeout(timeoutId), timer);
    }
}
