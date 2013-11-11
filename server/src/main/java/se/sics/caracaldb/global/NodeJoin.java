/* 
 * This file is part of the CaracalDB distributed storage system.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
