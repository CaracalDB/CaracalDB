/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.client;

import java.util.concurrent.BlockingQueue;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ClientWorkerInit extends Init<ClientWorker> {
    public final BlockingQueue<CaracalResponse> q;
    public final Address self;
    public final Address bootstrapServer;
    public final int sampleSize;
    
    public ClientWorkerInit(BlockingQueue<CaracalResponse> q, Address self, Address bootstrapServer, int sampleSize) {
        this.q = q;
        this.self = self;
        this.bootstrapServer = bootstrapServer;
        this.sampleSize = sampleSize;
    }
}
