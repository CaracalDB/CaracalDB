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
package se.sics.caracaldb.client;

import java.util.concurrent.BlockingQueue;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.kompics.Init;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ClientWorkerInit extends Init<ClientWorker> {
    public final BlockingQueue<CaracalResponse> q;
    public final BlockingQueue<DMMessage.Resp> dataModelQ;
    public final Address self;
    public final Address bootstrapServer;
    public final int sampleSize;
    
    public ClientWorkerInit(BlockingQueue<CaracalResponse> q, BlockingQueue<DMMessage.Resp> dataModelQ, Address self, Address bootstrapServer, int sampleSize) {
        this.q = q;
        this.dataModelQ = dataModelQ;
        this.self = self;
        this.bootstrapServer = bootstrapServer;
        this.sampleSize = sampleSize;
    }
}
