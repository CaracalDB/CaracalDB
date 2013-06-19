/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.bootstrap;

import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    // instance
    private Address self;
    private Configuration config;
    private RequestTimeout timeoutEvent;
    private State state;
    private LookupTable lut;
    
    private UUID timeoutId;

    public BootstrapClient(BootstrapCInit init) {
        final Negative<Bootstrap> bootstrap = provides(Bootstrap.class);
        final Positive<Timer> timer = requires(Timer.class);
        final Positive<Network> net = requires(Network.class);

        final Handler<Start> startHandler = new Handler<Start>() {
            @Override
            public void handle(Start event) {

                log.debug("Starting bootstrap client on {}", self);
                
                SchedulePeriodicTimeout spt =
                        new SchedulePeriodicTimeout(0, config.getKeepAlivePeriod());
                timeoutEvent = new RequestTimeout(spt);
                spt.setTimeoutEvent(timeoutEvent);
                trigger(spt, timer);
            }
        };
        subscribe(startHandler, control);

        final Handler<RequestTimeout> timeoutHandler = new Handler<RequestTimeout>() {
            @Override
            public void handle(RequestTimeout event) {
                if (timeoutId == null) {
                    timeoutId = event.getTimeoutId();
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
                    trigger(new CancelPeriodicTimeout(event.getTimeoutId()), timer);
                }
            }
        };
        subscribe(timeoutHandler, timer);

        final Handler<BootstrapResponse> responseHandler = new Handler<BootstrapResponse>() {
            @Override
            public void handle(BootstrapResponse event) {
                if (state == State.WAITING) {
                    try {
                        lut = LookupTable.deserialise(event.lut);
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
                        lut = LookupTable.deserialise(event.lut);
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
        subscribe(responseHandler, net);

        final Handler<BootUp> bootupHandler = new Handler<BootUp>() {
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
        subscribe(bootupHandler, net);

        final Handler<Stop> stopHandler = new Handler<Stop>() {
            @Override
            public void handle(Stop event) {
                // Just some cleanup
                trigger(new CancelPeriodicTimeout(timeoutEvent.getTimeoutId()), timer);
            }
        };
        subscribe(stopHandler, control);

        // INIT
        self = init.self;
        config = init.config;
        state = State.WAITING;
    }
}
