/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.operations;

import se.sics.kompics.Event;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class CaracalOp extends Event {

    public final long id;

    public CaracalOp(long id) {
        this.id = id;
    }
    
    /**
     * Return true if execution of op before this operation will affect this
     * operations result.
     * 
     * @param op
     * @return 
     */
    public abstract boolean affectedBy(CaracalOp op);
}
