/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.replication.log;

import com.google.common.collect.ComparisonChain;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class Value implements Comparable<Value> {
    
    public final long id;
    
    public Value(long id) {
        this.id = id;
    }

    /*
     * If the subclasses are otherwise incomparable use this to compare them instead
     */
    protected int baseCompareTo(Value that) {
        return ComparisonChain.start()
                .compare(this.getClass().getCanonicalName(), that.getClass().getCanonicalName())
                .compare(this.id, that.id).result();
    }
}
