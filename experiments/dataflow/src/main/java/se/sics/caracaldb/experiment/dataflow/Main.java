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
package se.sics.caracaldb.experiment.dataflow;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.MessageRegistrator;
import se.sics.kompics.Kompics;
import se.sics.kompics.network.Transport;

/**
 *
 * @author lkroll
 */
public class Main {

    public static volatile Address self;
    public static volatile long bufferSize = 1000 * 1000 * 1000; // 1GB
    public static volatile long minAlloc = 100 * 1000 * 1000; //100MB
    public static volatile long retryTime = 1000; // 1s
    public static volatile Transport protocol = Transport.TCP;
    public static volatile Sender sender = null;

    public static void main(String[] args) {
        if (args.length > 0) {
            if (args[0].equalsIgnoreCase("receive")) {
                if (args.length == 2) {
                    String ipS = args[1];
                    try {
                        self = Address.parseString(ipS);
                    } catch (Exception ex) {
                        System.err.println("Couldn't parse address. Exiting...");
                        System.exit(1);
                    }
                    startReceiver();
                } else {
                    System.err.println("Receive needs listen address arg.");
                    System.exit(1);
                }
            } else if (args[0].equalsIgnoreCase("send")) {
                if (args.length == 4) {
                    String ipS = args[1];
                    String ipD = args[2];
                    Address dst = null;
                    try {
                        self = Address.parseString(ipS);
                        dst = Address.parseString(ipD);
                    } catch (Exception ex) {
                        System.err.println("Couldn't parse address. Exiting...");
                        System.exit(1);
                    }

                    String fileS = args[3];
                    File f = new File(fileS);

                    if (!f.exists() || !f.canRead()) {
                        System.err.println("Can'y access file: " + f.getAbsolutePath());
                        System.exit(1);
                    }

                    startSender(f, dst);
                } else {
                    System.err.println("Send needs listen address, send address and filename args.");
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

    public static void startReceiver() {
        MessageRegistrator.register();
        Kompics.createAndStart(Receiver.class);
    }

    public static void startSender(File f, Address dst) {
        MessageRegistrator.register();
        Kompics.createAndStart(Sender.class);
        sender.startTransfer(f, dst);
    }

    static HashCode getHash(File f) {
        HashCode hc;
        try {
            hc = Files.hash(f, Hashing.sha1());
            //LOG.info("File {} has hash (SHA-1): {}", f, hc.toString());
            return hc;
        } catch (IOException ex) {
            System.err.println("Could not hash file " + f + ": " + ex);
            throw new RuntimeException(ex);
        }
    }

    static void completed() {
        ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
        ListenableFuture term = service.submit(new Callable() {
            @Override
            public Object call() {
                System.out.print("Nothing left to do...");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ex) {
                    System.out.println("");
                }
                System.out.println("shutting down Kompics.");
                Kompics.shutdown();
                System.out.println("Kompics shut down. Exiting...");
                System.exit(0);
                return null;
            }
        });

    }
}
