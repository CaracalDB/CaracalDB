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
package se.sics.caracaldb;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import java.util.Iterator;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class View implements Comparable<View> {

    public final ImmutableSortedSet<Address> members;
    public final int id;

    public View(ImmutableSortedSet<Address> members, int id) {
        this.members = members;
        this.id = id;
    }

    /**
     * Two Views are equivalent if they have the same members. (But not
     * necessarily the same id.)
     *
     * @param that
     * @return true if this is equivalent to that
     */
    public boolean equivalentTo(View that) {
        return Sets.symmetricDifference(this.members, that.members).isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof View) {
            View that = (View) o;

            return this.compareTo(that) == 0;
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 23 * hash + (this.members != null ? this.members.hashCode() : 0);
        hash = 23 * hash + this.id;
        return hash;
    }

    @Override
    public int compareTo(View that) {
        if (this.id != that.id) {
            return this.id - that.id;
        }
        if (this.members.size() != that.members.size()) {
            return this.members.size() - that.members.size();
        }
        Iterator<Address> thisIT, thatIT;
        thisIT = this.members.iterator();
        thatIT = that.members.iterator();
        while (thisIT.hasNext()) {
            Address thisVal = thisIT.next();
            Address thatVal = thatIT.next();
            int diff = thisVal.compareTo(thatVal);
            if (diff != 0) {
                return diff;
            }
        }
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("View(");
        sb.append(id);
        sb.append(", ");
        sb.append("{");
        for (Iterator<Address> it = members.iterator(); it.hasNext();) {
            Address adr = it.next();
            sb.append(adr.toString());
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append("}");
        return sb.toString();
    }
    
    public View copy() {
        return new View(ImmutableSortedSet.copyOf(members), id);
    }
    
    public SetView<Address> addedSince(View oldView) {
        return Sets.difference(members, oldView.members);
    }
    
    public SetView<Address> removedSince(View oldView) {
        return Sets.difference(oldView.members, members);
    }
}
