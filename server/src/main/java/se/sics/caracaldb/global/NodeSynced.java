/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.global;

import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class NodeSynced implements Maintenance {
    public final View view;
    public final KeyRange responsibility;

    public NodeSynced(View view, KeyRange responsibility) {
        this.view = view;
        this.responsibility = responsibility;
    }

    @Override
    public String toString() {
        return "NodeSynced("
                + view.toString() + ", "
                + responsibility.toString() + ")";
    }
}
