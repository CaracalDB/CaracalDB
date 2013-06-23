/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication;

import se.sics.caracaldb.View;
import se.sics.kompics.Event;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class ViewChange extends Event implements Comparable<ViewChange> {

    public final View view;
    public final int quorum;

    public ViewChange(View v, int quorum) {
        this.view = v;
        this.quorum = quorum;
    }

    @Override
    public int compareTo(ViewChange that) {
        int diff = this.view.compareTo(that.view);
        if (diff != 0) {
            return diff;
        }
        if (quorum != that.quorum) {
            return quorum - that.quorum;
        }
        return 0;
    }
}
