/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.log;

import se.sics.kompics.Event;

/**
 * Prune the ReplicatedLog up to <i>position</i>.
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Prune extends Event {
    public final long position;
    
    public Prune(long pos) {
        position = pos;
    }
}
