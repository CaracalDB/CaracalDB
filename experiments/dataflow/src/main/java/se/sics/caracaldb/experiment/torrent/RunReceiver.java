/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.experiment.torrent;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import se.sics.caracaldb.Address;
import se.sics.kompics.Kompics;

/**
 *
 * @author Alex Ormenisan <aaor@kth.se>
 */
public class RunReceiver {
    public static void main(String[] args) throws UnknownHostException {
        start();
        try {
            Kompics.waitForTermination();
        } catch (InterruptedException ex) {
            System.exit(1);
        }
    }

    public static void start() throws UnknownHostException {
        if (Kompics.isOn()) {
            Kompics.shutdown();
        }
        Address self = new Address(InetAddress.getLocalHost(), 25000, null);
        Address senderAdr = new Address(InetAddress.getLocalHost(), 15000, null);
        String outputDir = "./src/main/resources/torrent/downloader/";
        Receiver.Init init = new Receiver.Init(self, senderAdr, new File(outputDir), null);
        Kompics.createAndStart(Receiver.class, init, Runtime.getRuntime().availableProcessors(), 20); // Yes 20 is totally arbitrary
    }

    public static void stop() {
        Kompics.shutdown();
    }
}
