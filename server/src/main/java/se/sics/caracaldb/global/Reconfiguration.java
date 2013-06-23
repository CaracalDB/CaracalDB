/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.caracaldb.paxos.Decide;
import se.sics.caracaldb.replication.ViewChange;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Reconfiguration extends Decide implements Maintenance {
    
    public final ViewChange change;
    
    public Reconfiguration(ViewChange change) {
        this.change = change;
    }

    @Override
    public int compareTo(Decide o) {
        if (o instanceof Reconfiguration) {
            Reconfiguration that = (Reconfiguration) o;
            int diff = this.change.compareTo(that.change);
            return diff;
        }
        return super.baseCompareTo(o);
    }
    
    @Override
    public String toString() {
        return "Reconfiguration(" + change.toString() + ")";        
    }
    
}
