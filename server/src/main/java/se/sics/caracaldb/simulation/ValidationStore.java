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
package se.sics.caracaldb.simulation;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import junit.framework.Assert;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.ResponseCode;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ValidationStore {
    private Map<Long, CaracalOp> requests = new TreeMap<Long, CaracalOp>();
    private Map<Long, CaracalResponse> responses = new TreeMap<Long, CaracalResponse>();
    
    public synchronized void request(CaracalOp op) {
        requests.put(op.id, op);
    }
    
    public synchronized void response(CaracalResponse res) {
        responses.put(res.id, res);
    }
    
    public synchronized void validate() {
        for (Entry<Long, CaracalOp> e : requests.entrySet()) {
            Long id = e.getKey();
            CaracalOp req = e.getValue();
            CaracalResponse resp = responses.get(id);
            Assert.assertNotNull("Request " + id + " did not get a response!", resp);
            Assert.assertTrue("Request " + id + " was not successful!", resp.code == ResponseCode.SUCCESS);
        }
    }
    
    public synchronized void print() {
        StringBuilder sb = new StringBuilder();
        sb.append("##### Validation Store ##### \n");
        for (Entry<Long, CaracalOp> e : requests.entrySet()) {
            Long id = e.getKey();
            CaracalOp req = e.getValue();
            CaracalResponse resp = responses.get(id);
            sb.append(req);
            sb.append(" --> ");
            sb.append(resp);
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }
    
    public synchronized boolean isDone() {
        return requests.size() == responses.size();
    }
}
