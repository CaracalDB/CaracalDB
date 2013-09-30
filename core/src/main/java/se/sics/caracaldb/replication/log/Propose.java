/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.log;

import se.sics.kompics.Event;

/**
 *
 * Propose the Value with 
 * 
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Propose<T extends Value> extends Event {
    
    public final T value;
    
    public Propose(T value) {
        this.value = value;
    }
    
}
