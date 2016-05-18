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
package se.sics.caracaldb.experiment.randomdata;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Positive;
import se.sics.kompics.Start;
import se.sics.kompics.network.Network;

/**
 *
 * @author lkroll
 */
public class Receiver extends ComponentDefinition {

    static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
    // Ports
    Positive<Network> net = requires(Network.class);
    // Instance
    private final Address self;
    private final File statsDir;
    private final HashMap<Address, ConnectionStatistics> stats = new HashMap<>();
    private long offset = -1;

    public Receiver() {
        Address parentAddress = config().getValue("experiment.self.address", Address.class);
        byte[] id = RandomDataSerializer.uuid2Bytes(this.id());
        self = parentAddress.newVirtual(id);
        statsDir = config().getValueOrDefault("experiment.stats.dir", new File("."));
        LOG.info("Using {} for statistics data.", statsDir);

        // subscriptions
        subscribe(startHandler, control);
        subscribe(messageHandler, net);
    }

    Handler<Start> startHandler = new Handler<Start>() {

        @Override
        public void handle(Start event) {
            LOG.info("{}: Waiting for incoming data transfer.", self);
        }
    };

    Handler<RandomDataMessage> messageHandler = new Handler<RandomDataMessage>() {

        @Override
        public void handle(RandomDataMessage event) {
            if (offset < 0) {
                offset = event.arrivalTime;
            }
            ConnectionStatistics cs = stats.get(event.getSource());
            if (cs == null) {
                cs = new ConnectionStatistics(event.getSource());
                stats.put(event.getSource(), cs);
            }
            cs.add(event);
        }
    };

    public class ConnectionStatistics {

        private final TreeMap<Long, Long> counters = new TreeMap<Long, Long>();
        private long lastWindow = 0;
        private final String fileName;

        private ConnectionStatistics(Address source) {
            fileName = statsDir.getAbsolutePath() + File.pathSeparator + source.toString() + File.separator + ".data";
        }

        void add(RandomDataMessage event) {
            long adjustedTime = (event.arrivalTime - offset) / 1000l; // in seconds since first message
            long currentCounter = counters.getOrDefault(adjustedTime, 0l);
            currentCounter++;
            counters.put(adjustedTime, currentCounter);

            if (adjustedTime > lastWindow) {
                compact(lastWindow, adjustedTime);
                lastWindow = adjustedTime;
            }
        }

        void compact(long from, long until) {
            try (FileWriter writer = new FileWriter(fileName, true)) {

                for (long window = from; window < until; window++) {
                    Long counterL = counters.remove(window);
                    if (counterL != null) {
                        long counter = counterL;
                        writer.append(String.format("%d, %d\n", window, counter));
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

}
