/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.log;

import se.sics.kompics.Event;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Decide<T extends Value> extends Event {
    public final long position;
    public final T value;
    
    public Decide(long position, T value) {
        this.position = position;
        this.value = value;
    }
}
