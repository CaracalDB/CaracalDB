/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos;

import se.sics.kompics.Event;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Propose extends Event {
    
    public final long id;
    public final Decide event;
    
    public Propose(long id, Decide event) {
        this.id = id;
        this.event = event;
    }
    
}
