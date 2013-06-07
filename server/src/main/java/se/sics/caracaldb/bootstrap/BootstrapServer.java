/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.bootstrap;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.global.LookupTable;
import se.sics.caracaldb.system.Configuration;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
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

    public BootstrapServer(BootstrapSInit init) {

        final Handler<Start> startHandler = new Handler<Start>() {
            @Override
            public void handle(Start event) {

                long keepAliveTimeout = config.getKeepAlivePeriod() * 2;
                SchedulePeriodicTimeout spt =
                        new SchedulePeriodicTimeout(keepAliveTimeout, keepAliveTimeout);
                spt.setTimeoutEvent(new ClearTimeout(spt));
                trigger(spt, timer);
                active.add(self);
            }
        };
        subscribe(startHandler, control);

        final Handler<BootstrapRequest> requestHandler = new Handler<BootstrapRequest>() {
            @Override
            public void handle(BootstrapRequest event) {
                if (state != State.DONE) {
                    fresh.add(event.getSource());
                    if (!active.contains(event.getSource())) {
                        log.debug("Got BootstrapRequest from {}", event.getSource());
                    }
                }
                if (state == State.SEEDING) {
                    trigger(new BootstrapResponse(self, event.getSource(), lutData), net);
                }
            }
        };
        subscribe(requestHandler, net);

        final Handler<Ready> readyHandler = new Handler<Ready>() {
            @Override
            public void handle(Ready event) {
                if (state == State.SEEDING) {
                    readySet.add(event.getSource());
                }
            }
        };
        subscribe(readyHandler, net);

        final Handler<ClearTimeout> timeoutHandler = new Handler<ClearTimeout>() {
            @Override
            public void handle(ClearTimeout event) {
                if (state == State.COLLECTING) {
                    active.clear();
                    active.addAll(fresh);
                    fresh.clear();
                    active.add(self);
                    log.debug("Cleaning up. {} hosts in the active set.", active.size());
                    if (active.size() >= config.getBootThreshold()) {
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
        subscribe(timeoutHandler, timer);

        // INIT
        self = init.self;
        config = init.config;
        state = State.COLLECTING;
    }

    private void bootUp() {
        log.info("Threshold reached. Seeding LUT.");
        bootSet = ImmutableSet.copyOf(active);
        waitSet = ImmutableSet.copyOf(active);
        lut = LookupTable.generateInitial(bootSet);
        try {
            lutData = lut.serialise();
        } catch (IOException ex) {
            log.error("Fatal boot error: Could not generate LookupTable. Shutting down.", ex);
            System.exit(1);
        }

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
    }
}
