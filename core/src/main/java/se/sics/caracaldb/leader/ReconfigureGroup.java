/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.leader;

import se.sics.caracaldb.View;
import se.sics.kompics.Event;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ReconfigureGroup extends Event {
    public final View view;
    public final int quorum;

    public ReconfigureGroup(View v, int quorum) {
        this.view = v;
        this.quorum = quorum;
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
