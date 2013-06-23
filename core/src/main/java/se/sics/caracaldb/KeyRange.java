/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
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
