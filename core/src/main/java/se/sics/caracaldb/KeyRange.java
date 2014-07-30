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

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KeyRange defines an interval with bounds.
 * <p>
 * Use like KeyRange.open(lowerBound).closed(upperBound) to get an instance
 * where lowerBound and upperBound are Key objects.
 * <p>
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class KeyRange {

    private static final Logger LOG = LoggerFactory.getLogger(KeyRange.class);

    public static enum Bound {

        OPEN,
        CLOSED;
    }
    private static final Bound OPEN = Bound.OPEN;
    private static final Bound CLOSED = Bound.CLOSED;
    public static final KeyRange EMPTY = new KeyRange(OPEN, Key.ZERO_KEY, Key.ZERO_KEY, OPEN);
    public final Bound beginBound;
    public final Key begin;
    public final Key end;
    public final Bound endBound;

    public KeyRange(Bound beginBound, Key begin, Key end, Bound endBound) {
        this.beginBound = beginBound;
        this.begin = begin;
        this.end = end;
        this.endBound = endBound;
    }

    /**
     * Create a KeyRange that only includes key
     * @param key
     * @return 
     */
    public static KeyRange key(Key key) {
        return new KeyRange(CLOSED, key, key, CLOSED);
    }

    public static KRBuilder open(Key begin) {
        return new KRBuilder(OPEN, begin);
    }

    public static KRBuilder closed(Key begin) {
        return new KRBuilder(CLOSED, begin);
    }

    public static KeyRange prefix(Key prefix) {
        return KeyRange.closed(prefix).open(prefix.inc());
    }

    public static KRBuilder startFrom(KeyRange kr) {
        return new KRBuilder(kr.beginBound, kr.begin);
    }
    
    public static boolean overlap(KeyRange a, KeyRange b) {
        if (a.equals(EMPTY) || b.equals(EMPTY)) {
            return false;
        }
        if (a.contains(b.begin) || a.contains(b.end)) {
            return true;
        }
        if (b.contains(a.begin) || b.contains(a.end)) {
            return true;
        }
        return false;
    }

    public KeyRange intersect(KeyRange that) {
        if (this.equals(EMPTY) || that.equals(EMPTY)) {
            return EMPTY;
        }
        if (this.equals(that)) {
            return this;
        }
        if (this.contains(that.begin)) {
            if (this.contains(that.end)) { //case this = [a,d], that = [b,c]
                return that;
            } else if (that.contains(this.end)) { //case this = [a,c], that = [b,d]
                return new KeyRange(that.beginBound, that.begin, this.end, this.endBound);
            } else { //case this = [a,c) and that = [b,c)
                return that;
            }
        } else if (that.contains(this.begin)) {
            if (that.contains(this.end)) { //case this = [b,c], that = [a,d]
                return this;
            } else if (this.contains(that.end)) { //case this = [b,d], that = [a,c]
                return new KeyRange(this.beginBound, this.begin, that.end, that.endBound);
            } else { // case this = [b,c) and that = [a,c)
                return this;
            }
        } else { //case this = (a,.. and that = (a,..
            if (this.contains(that.end)) { //case this = (a,c], that = (a,b]
                return that;
            } else if (that.contains(this.end)) { //case this = (a,b], that = (a,c]
                return this;
            } else {
                /*
                 * case this = (a,b) and that = (a,b), should have been caught
                 * in begining at equals test, which means equals is broken
                 */
                LOG.error("KeyRange equals method is broken");
                System.exit(1);
                return EMPTY;
            }
        }
    }
    
    public boolean overlapsWith(KeyRange that) {
        return KeyRange.overlap(this, that);
    }

    public boolean isKey() {
        return begin.equals(end) && beginBound.equals(Bound.CLOSED) && endBound.equals(Bound.CLOSED);
    }

    /**
     * Checks for inclusion
     * <p>
     * @param k
     * @return true if k in this interval
     */
    public boolean contains(Key k) {
        if ((beginBound == OPEN) && k.equals(begin)) {
            return false;
        }

        if ((endBound == OPEN) && k.equals(end)) {
            return false;
        }

        if ((begin.leq(k)) && (k.leq(end))) {
            return true;
        }

        return false;
    }

    /**
     * Checks for inclusion without the overhead of wrapping into an object.
     * <p>
     * @param key
     * @return true if key in this interval
     */
    public boolean contains(byte[] key) {
        if (beginBound == OPEN && begin.equals(key)) {
            return false;
        }

        if (endBound == OPEN && end.equals(key)) {
            return false;
        }

        if (begin.leq(key) && end.geq(key)) {
            return true;
        }

        return false;
    }

    /**
     * Checks for inclusion
     * <p>
     * @param that
     * @return true if that range is included in this range
     */
    public boolean contains(KeyRange that) {
        if (this.equals(that)) {
            return true;
        }
        if (this.contains(that.begin)) {
            if (this.contains(that.end)) {
                return true;
            } else if (this.end.equals(that.end) && that.endBound.equals(Bound.OPEN)) {
                //case this = [a,c), that = [b,c)
                return true;
            } else {
                return false;
            }
        } else if (this.begin.equals(that.begin) && that.beginBound.equals(Bound.OPEN)) {
            //case this = (a,.., that = (a,..
            if (this.contains(that.end)) {
                return true;
            } else if (this.end.equals(that.end) && that.endBound.equals(Bound.OPEN)) {
                /*
                 * case this = (a,b), that = (a,b) should return true, but it
                 * should have done so in the equals test in beginning which
                 * means the equals is broken
                 */
                LOG.error("KeyRange equals method is broken");
                System.exit(1);
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof KeyRange) {
            KeyRange that = (KeyRange) o;
            if (!Objects.equal(this.beginBound, that.beginBound)) {
                return false;
            }
            if (!Objects.equal(this.begin, that.begin)) {
                return false;
            }
            if (!Objects.equal(this.end, that.end)) {
                return false;
            }
            if (!Objects.equal(this.endBound, that.endBound)) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(beginBound, begin, end, endBound);
    }

    @Override
    public String toString() {
        String str = (beginBound == OPEN) ? "(" : "[";
        str += begin.toString() + ", ";
        str += end.toString();
        str += (endBound == OPEN) ? ")" : "]";
        return str;
    }

    public static class KRBuilder {

        private Bound beginBound;
        private Key begin;

        public KRBuilder(Bound beginBound, Key begin) {
            this.beginBound = beginBound;
            this.begin = begin;
        }

        public KeyRange open(Key end) {
            return new KeyRange(beginBound, begin, end, OPEN);
        }

        public KeyRange closed(Key end) {
            return new KeyRange(beginBound, begin, end, CLOSED);
        }

        public KeyRange endFrom(KeyRange kr) {
            return new KeyRange(beginBound, begin, kr.end, kr.endBound);
        }
    }
}
