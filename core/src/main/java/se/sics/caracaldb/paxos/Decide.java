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
public abstract class Decide extends Event implements Comparable<Decide> {

    /*
     * If the subclasses are otherwise incomparable use this to compare them instead
     */
    protected int baseCompareTo(Decide o) {
        return this.getClass().getCanonicalName().compareTo(o.getClass().getCanonicalName());
    }
}
