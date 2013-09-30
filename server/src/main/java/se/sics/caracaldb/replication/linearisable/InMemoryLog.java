/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.linearisable;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.PutRequest;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class InMemoryLog implements OperationsLog {
    
    private final LinkedList<CaracalOp> log = new LinkedList<CaracalOp>();

    @Override
    public void append(CaracalOp op) {
        log.add(op);
    }

    @Override
    public void clear() {
        log.clear();
    }

    @Override
    public Iterator<CaracalOp> iterator() {
        return log.iterator();
    }

    @Override
    public List<CaracalOp> getApplicable(Key k) {
        List<CaracalOp> replayLog = new LinkedList<CaracalOp>();
        for (CaracalOp op : log) {
            if (op instanceof PutRequest) {
                PutRequest pr = (PutRequest) op;
                if (k.equals(pr.key)) {
                    replayLog.add(op);
                }
            }
        }
        return replayLog;
    }
    
    

    @Override
    public boolean isEmpty() {
        return log.isEmpty();
    }
    
}
