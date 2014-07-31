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
package se.sics.caracaldb.replication.log;

import com.google.common.collect.ComparisonChain;
import java.util.UUID;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.View;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Reconfigure extends Value {

    public final View view;
    public final int quorum;
    public final int versionId;
    public final KeyRange responsibility;

    public Reconfigure(UUID id, View v, int quorum, int versionId, KeyRange responsibility) {
        super(id);
        this.view = v;
        this.quorum = quorum;
        this.versionId = versionId;
        this.responsibility = responsibility;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Reconfigure) {
            Reconfigure that = (Reconfigure) o;
            return this.compareTo(that) == 0;
        }
        return false;
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
                .compare(this.versionId, that.versionId)
                //.compare(this.responsibility, that.responsibility) FIXME
                .result();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Reconfigure(");
        sb.append("\n     View: ");
        sb.append(view);
        
        sb.append(quorum);
        sb.append("\n     Version: ");
        sb.append(versionId);
        sb.append("\n     Range: ");
        sb.append(responsibility);
        sb.append("\n     )");
        return sb.toString();
    }
}
