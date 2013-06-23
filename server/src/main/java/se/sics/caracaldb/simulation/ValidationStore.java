/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
