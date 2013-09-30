/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.linearisable;

import java.util.List;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.operations.CaracalOp;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface OperationsLog extends Iterable<CaracalOp> {
    public void append(CaracalOp op);
    public void clear();
    public boolean isEmpty();
    public List<CaracalOp> getApplicable(Key k);
}
