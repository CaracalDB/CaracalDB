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
public abstract class Commands {

    public static class Start extends Event {

        public final int num;

        public Start(int num) {
            this.num = num;
        }
    }

    public static class Verify extends Event {
    }

    public static class Operation extends Event {

    }
    
    public static class ChurnEvent extends Event {
    }
    
    public static class Join extends ChurnEvent {
        
    }
    
    public static class Fail extends ChurnEvent {
        
    }
}
