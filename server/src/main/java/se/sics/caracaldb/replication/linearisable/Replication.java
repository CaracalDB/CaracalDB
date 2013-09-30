/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.linearisable;

import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.kompics.PortType;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Replication extends PortType {

    {
        request(CaracalOp.class);
        request(ViewChange.class);
        indication(CaracalResponse.class);
        indication(Synced.class);
    }
}
