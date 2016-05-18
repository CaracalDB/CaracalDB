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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.kompics.Kompics;
import se.sics.kompics.config.Config;
import se.sics.kompics.config.ConfigUpdate;
import se.sics.kompics.config.TypesafeConfig;
import se.sics.kompics.config.ValueMerger;
import se.sics.kompics.network.Transport;

/**
 *
 * @author lkroll
 */
public class Main {
//    private static final long MB = 1000l * 1000l;
//    private static final long GB = 1000l * 1000l * 1000l;

    static final Logger LOG = LoggerFactory.getLogger(Main.class);

    static Config conf;
    static Config.Builder confB;

    public static void main(String[] args) {
        conf = TypesafeConfig.load();
        confB = conf.modify(UUID.randomUUID());
        if (args.length > 0) {
            if (args[0].equalsIgnoreCase("receive")) {
                if (args.length == 3) {
                    String protoS = args[1];
                    Transport proto = Transport.valueOf(protoS);
                    String ipS = args[2];
                    try {
                        Address self = Address.parseString(ipS);
                        confB.setValue("experiment.self.address", self);
                    } catch (UnknownHostException | ParseException ex) {
                        LOG.error("Couldn't parse address. Exiting...");
                        System.exit(1);
                    }
                    try {
                        startReceiver();
                    } catch (InterruptedException | ExecutionException ex) {
                        LOG.error("Start of Receiver failed: {}", ex);
                        System.exit(1);
                    }
                } else {
                    System.err.println("Receive needs protocol and listen address args.");
                    System.exit(1);
                }
            } else if (args[0].equalsIgnoreCase("send")) {
                if (args.length == 4) {
                    String protoS = args[1];
                    Transport proto = Transport.valueOf(protoS);
                    String ipS = args[2];
                    String ipD = args[3];
                    Address dst = null;
                    try {
                        Address self = Address.parseString(ipS);
                        dst = Address.parseString(ipD);
                        confB.setValue("experiment.self.address", self);
                    } catch (UnknownHostException | ParseException ex) {
                        LOG.error("Couldn't parse address. Exiting...");
                        System.exit(1);
                    }

                    try {
                        startSender(dst, proto);
                    } catch (InterruptedException | ExecutionException ex) {
                        LOG.error("Start of Sender failed: {}", ex);
                        System.exit(1);
                    }
                } else {
                    System.err.println("Send needs protocol, listen address, and send address args.");
                    System.exit(1);
                }
            } else {
                System.err.println("Unknown command: " + args[0]);
                System.exit(1);
            }
        } else {
            System.out.println("No input. Exiting...");
            System.exit(0);
        }
    }

    public static Manager createManager() throws InterruptedException, ExecutionException {
        MessageRegistrator.register();
        SettableFuture<Manager> managerFuture = SettableFuture.create();

        ConfigUpdate up = confB.finalise();
        Config.Impl configI = (se.sics.kompics.config.Config.Impl) conf.copy(false);
        configI.apply(up, ValueMerger.NONE);
        conf = configI;
        System.out.println("Address: " + conf.getValue("experiment.self.address", Address.class));
        Kompics.setConfig(conf);
        Kompics.createAndStart(Manager.class, new Manager.Init(managerFuture));
        LOG.debug("Waiting for Manager to start...");
        Manager m = managerFuture.get();
        LOG.info("Manager started!");
        return m;
    }

    public static void startReceiver() throws InterruptedException, ExecutionException {
        Manager m = createManager();
        m.startReceiver();
        LOG.info("Requesting Receiver start.");
    }

    public static void startSender(Address dst, Transport proto) throws InterruptedException, ExecutionException {
        Manager m = createManager();
        ListenableFuture<UUID> senderIdF = m.startSender(dst, proto);
        LOG.debug("Requesting Sender start...");
        UUID senderId = senderIdF.get();
        LOG.info("Sender {} started! Sender to {} via {}", senderId, dst, proto);
    }

}
