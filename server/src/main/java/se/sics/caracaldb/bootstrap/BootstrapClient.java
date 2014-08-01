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

import java.io.IOException;
import java.util.UUID;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.global.LookupTable;
import se.sics.caracaldb.system.Configuration;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.Stop;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Network;
import se.sics.kompics.timer.CancelPeriodicTimeout;
import se.sics.kompics.timer.SchedulePeriodicTimeout;
import se.sics.kompics.timer.Timer;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class BootstrapClient extends ComponentDefinition {

    public static enum State {

        WAITING, READY, STARTED;
    }
    // static
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(BootstrapClient.class);
    // ports
    final Negative<Bootstrap> bootstrap = provides(Bootstrap.class);
    final Positive<Timer> timer = requires(Timer.class);
    final Positive<Network> net = requires(Network.class);
    // instance
    private Address self;
    private Configuration config;
    private RequestTimeout timeoutEvent;
    private State state;
    private LookupTable lut;

    private UUID timeoutId;

    public BootstrapClient(BootstrapCInit init) {
        // INIT
        self = init.self;
        config = init.config;
        state = State.WAITING;
        // subscriptions
        subscribe(startHandler, control);
        subscribe(stopHandler, control);
        subscribe(timeoutHandler, timer);
        subscribe(brspHandler, net);
        subscribe(bootHandler, net);
    }

    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            log.debug("Starting bootstrap client on {}", self);

            SchedulePeriodicTimeout spt
                    = new SchedulePeriodicTimeout(0,
                            config.getMilliseconds("caracal.network.keepAlivePeriod"));
            timeoutEvent = new RequestTimeout(spt);
            spt.setTimeoutEvent(timeoutEvent);
            trigger(spt, timer);
        }
    };
    Handler<RequestTimeout> timeoutHandler = new Handler<RequestTimeout>() {

        @Override
        public void handle(RequestTimeout e) {
            if (timeoutId == null) {
                timeoutId = e.getTimeoutId();
            }

            if (state == State.WAITING) {
                trigger(new BootstrapRequest(self, config.getBootstrapServer()), net);
            } else if (state == State.READY) {
                trigger(new Ready(self, config.getBootstrapServer()), net);
            } else if ((state == State.STARTED) && (lut == null)) {
                // System was booted without us, so just keep asking
                trigger(new BootstrapRequest(self, config.getBootstrapServer()), net);
            } else {
                // this should have been done already, actually
                trigger(new CancelPeriodicTimeout(e.getTimeoutId()), timer);
            }
        }
    };
    Handler<BootstrapResponse> brspHandler = new Handler<BootstrapResponse>() {

        @Override
        public void handle(BootstrapResponse e) {
            if (state == State.WAITING) {
                try {
                    lut = LookupTable.deserialise(e.lut);
                    trigger(new Ready(self, config.getBootstrapServer()), net);
                    state = State.READY;
                } catch (IOException ex) {
                    log.error("Could not deserialise LookupTable.", ex);
                    // Just wait for the server to resend it. 
                    // If this problem keeps coming up I guess something is wrong ;)
                }
            }
            if ((state == State.STARTED) && (lut == null)) {
                // The other nodes are already up, so we can start immediately
                try {
                    lut = LookupTable.deserialise(e.lut);
                    trigger(new Bootstrapped(lut), bootstrap);
                } catch (IOException ex) {
                    log.error("Could not deserialise LookupTable.", ex);
                    // Just wait for the server to resend it. 
                    // If this problem keeps coming up I guess something is wrong ;)
                }
            }
            // If LUT is already there, just ignore the duplicate    
        }
    };
    Handler<BootUp> bootHandler = new Handler<BootUp>() {

        @Override
        public void handle(BootUp event) {
            if (state == State.READY) {
                log.info("{} Booting up.", self);
                trigger(new Bootstrapped(lut), bootstrap);
                trigger(new CancelPeriodicTimeout(timeoutId), timer);
            } else {
                state = State.STARTED;
            }
        }
    };
    Handler<Stop> stopHandler = new Handler<Stop>() {

        @Override
        public void handle(Stop event) {
            // Just some cleanup
            trigger(new CancelPeriodicTimeout(timeoutEvent.getTimeoutId()), timer);
        }
    };
}
