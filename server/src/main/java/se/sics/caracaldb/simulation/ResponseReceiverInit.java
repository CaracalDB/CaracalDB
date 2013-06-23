/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.simulation;

import se.sics.kompics.Init;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ResponseReceiverInit extends Init<ResponseReceiver> {
    public final ValidationStore store;
    public ResponseReceiverInit(ValidationStore store) {
        this.store = store;
    }
}
