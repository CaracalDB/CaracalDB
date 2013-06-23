/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class MaintenanceMsg extends Message {
    public final Maintenance op;
    
    public MaintenanceMsg(Address src, Address dst, Maintenance op) {
        super(src, dst);
        this.op = op;
    }
    
    @Override
    public String toString() {
        return "MaintenanceMsg(" 
                + this.getSource().toString() + ", "
                + this.getDestination().toString() + ", "
                + op.toString() + ")";
    }
}
