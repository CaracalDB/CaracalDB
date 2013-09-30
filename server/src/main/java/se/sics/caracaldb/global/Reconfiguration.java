/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.caracaldb.replication.log.Value;
import se.sics.caracaldb.replication.linearisable.ViewChange;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Reconfiguration implements Maintenance {
    
    public final ViewChange change;
    
    public Reconfiguration(ViewChange change) {
        this.change = change;
    }
    
    @Override
    public String toString() {
        return "Reconfiguration(" + change.toString() + ")";        
    }
    
}
