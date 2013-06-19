/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.paxos;

import java.util.Iterator;
import se.sics.caracaldb.View;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Reconfigure extends Decide {
    public final View view;
    public final int quorum;
    
    public Reconfigure(View v, int quorum) {
        this.view = v;
        this.quorum = quorum;
    }

    @Override
    public int compareTo(Decide o) {
        if (o instanceof Reconfigure) {
            Reconfigure that = (Reconfigure) o;
            int diff = this.view.compareTo(that.view);
            if (diff != 0) {
                return diff;
            }
            if (quorum != that.quorum) {
                return quorum - that.quorum;
            }
            return 0;
        }
        return super.baseCompareTo(o);
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Reconfigure(");
        sb.append("\n     View: ");
        sb.append(view);
        sb.append("\n     Quorum: ");
        sb.append(quorum);
        sb.append("\n     )");
        return sb.toString();
    }
}
