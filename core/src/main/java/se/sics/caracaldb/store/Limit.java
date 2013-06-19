/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Limit {
    public static enum Type {
        COUNT,
        BYTES;
    }
    
    public final Type type;
    public final int value;
    
    public Limit(Type t, int value) {
        type = t;
        this.value = value;
    }
    
    public static Limit toBytes(int number) {
        return new Limit(Type.BYTES, number);
    }
    
    public static Limit toKiloBytes(int number) {
        return new Limit(Type.BYTES, number*1000);
    }
    
    public static Limit toMegaBytes(int number) {
        return new Limit(Type.BYTES, number*1000*1000);
    }
    
    public static Limit toItems(int number) {
        return new Limit(Type.COUNT, number);
    }
}

