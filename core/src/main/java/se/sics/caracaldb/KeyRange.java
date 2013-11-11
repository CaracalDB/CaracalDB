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

/**
 *  KeyRange defines an interval with bounds.
 * 
 * Use like KeyRange.open(lowerBound).closed(upperBound) to get an instance
 * where lowerBound and upperBound are Key objects.
 * 
 * @author Lars Kroll <lkroll@sics.se>
 */
public class KeyRange {
    
    public static enum Bound {
        OPEN,
        CLOSED;
    }
    
    private static Bound OPEN = Bound.OPEN;
    private static Bound CLOSED = Bound.CLOSED;
    
    public final Bound beginBound;
    public final Key begin;
    public final Key end;
    public final Bound endBound;
    
    private KeyRange(Bound beginBound, Key begin, Key end, Bound endBound) {
        this.beginBound = beginBound;
        this.begin = begin;
        this.end = end;
        this.endBound = endBound;
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
    
    /**
     * Checks for inclusion
     * 
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
     * 
     * @param key
     * @return true if key in this interval
     */
    public boolean contains(byte[] key) {
        if ((beginBound == OPEN) && begin.equals(key)) {
            return false;
        }
        
        if ((endBound == OPEN) && end.equals(key)) {
            return false;
        }
        
        if (Key.leq(begin.getArray(), key) && Key.leq(end.getArray(), key)) {
            return true;
        }
        
        return false;
    }
    
    public KRBuilder startFrom(KeyRange kr) {
        return new KRBuilder(kr.beginBound, kr.begin);
    }
    
    @Override
    public String toString() {
        String str = (beginBound == OPEN) ? "(" : "[";
        str += begin.toString() + ", ";
        str += end.toString();
        str += (endBound == OPEN) ? ")" : "]";
        return str;
    }
    
}
