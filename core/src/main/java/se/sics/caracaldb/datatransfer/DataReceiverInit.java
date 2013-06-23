/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.datatransfer;

import se.sics.kompics.Init;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class DataReceiverInit extends Init<DataReceiver> {
    public final InitiateTransfer event;
    public final boolean force;
    
    public DataReceiverInit(InitiateTransfer event, boolean force) {
        this.event = event;
        this.force = force;
    }
}
