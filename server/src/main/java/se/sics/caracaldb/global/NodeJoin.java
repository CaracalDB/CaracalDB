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
public class NodeJoin implements Maintenance {

    public final View view;
    public final KeyRange responsibility;
    public final boolean dataTransfer;
    public final int quorum;

    public NodeJoin(View view, int quorum, KeyRange responsibility, boolean dataTransfer) {
        this.view = view;
        this.responsibility = responsibility;
        this.dataTransfer = dataTransfer;
        this.quorum = quorum;
    }

    @Override
    public String toString() {
        return "NodeJoin("
                + view.toString() + ", "
                + quorum + ","
                + responsibility.toString() + ", "
                + dataTransfer + ")";
    }
}
