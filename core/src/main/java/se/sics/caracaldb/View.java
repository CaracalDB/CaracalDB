/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb;

import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.TreeSet;
import se.sics.kompics.address.Address;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class View implements Comparable<View> {

    // Should be Immutable, but tell that to Kryo -.-
    public final NavigableSet<Address> members;
    public final int id;

    public View(ImmutableSortedSet<Address> members, int id) {
        this.members = new TreeSet<Address>(members);
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
}
