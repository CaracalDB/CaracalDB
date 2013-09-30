/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.log;

import com.google.common.collect.ComparisonChain;
import se.sics.caracaldb.View;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Reconfigure extends Value {

    public final View view;
    public final int quorum;

    public Reconfigure(long id, View v, int quorum) {
        super(id);
        this.view = v;
        this.quorum = quorum;
    }

    @Override
    public int compareTo(Value o) {
        int superRes = super.baseCompareTo(o);
        if (superRes != 0) {
            return superRes;
        }
        // I can do this because baseCompareTo already checks for class equality
        Reconfigure that = (Reconfigure) o; 
        return ComparisonChain.start()
                .compare(this.view, that.view)
                .compare(this.quorum, that.quorum)
                .result();
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
