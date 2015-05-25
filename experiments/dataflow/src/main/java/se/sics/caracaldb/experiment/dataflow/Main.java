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
import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import se.sics.caracaldb.Address;
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
    public static volatile long maxAlloc = 512 * 1000 * 1000; //512MB
    public static volatile long retryTime = 1000; // 1s
    public static volatile Transport protocol = Transport.TCP;
    public static volatile Sender sender = null;
    public static final BlockingQueue<TransferStats> resultQ = new LinkedBlockingQueue<TransferStats>(1);

    public static void main(String[] args) {
        String proto = System.getProperty("protocol");
        if (proto != null) {
            protocol = Transport.valueOf(proto);
        }
        System.out.println("\n******* Using " + protocol + " ************\n");
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
        try {
            MessageRegistrator.register();
            Kompics.createAndStart(Receiver.class);
            cleanup(resultQ.take());
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void startSender(File f, Address dst) {
        MessageRegistrator.register();
        Kompics.createAndStart(Sender.class);
        ListenableFuture<StatsWithRTTs> promise = sender.startTransfer(f, dst);
        try {
            cleanup(promise.get().stats);
        } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        } catch (ExecutionException ex) {
            throw new RuntimeException(ex);
        }
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

    private static void cleanup(TransferStats ts) {
        System.out.println("Finished transfer " + ts + " cleaning up...");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ex) {
            System.out.println(ex);
        }
        System.out.println("Shutting down Kompics...");
        Kompics.shutdown();
        System.out.println("Kompics shut down. Exiting...");
        System.exit(0);
    }

//    static void completed() {
//        ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));
//        ListenableFuture term = service.submit(new Callable() {
//            @Override
//            public Object call() {
//                System.out.print("Nothing left to do...");
//                try {
//                    Thread.sleep(5000);
//                } catch (InterruptedException ex) {
//                    System.out.println("");
//                }
//                System.out.println("shutting down Kompics.");
//                Kompics.shutdown();
//                System.out.println("Kompics shut down. Exiting...");
//                System.exit(0);
//                return null;
//            }
//        });
//
//    }
}
