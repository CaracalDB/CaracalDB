/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.simulation.command;

import se.sics.kompics.Event;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class BootCmd extends Event {
    public final int nodeCount;
    
    public BootCmd(int nodeCount) {
        this.nodeCount = nodeCount;
    }
}
