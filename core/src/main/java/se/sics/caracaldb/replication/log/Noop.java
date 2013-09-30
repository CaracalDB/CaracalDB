/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.log;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public final class Noop extends Value {
    
    public static final Noop val = new Noop();
    
    private Noop() {
        super(0);
    }

    @Override
    public int compareTo(Value o) {
        return super.baseCompareTo(o);
    }
    
}
