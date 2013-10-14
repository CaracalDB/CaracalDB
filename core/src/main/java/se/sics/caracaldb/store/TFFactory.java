/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import org.javatuples.Pair;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class TFFactory {
    public static TransformationFilter tombstoneFilter() {
        return new TransformationFilter() {
            @Override
            public Pair<Boolean, byte[]> execute(byte[] serializedValue) {
                return new Pair<Boolean, byte[]>(serializedValue == null, serializedValue);
            }
        };
    }
    
    public static TransformationFilter noTF() {
        return new TransformationFilter() {
            @Override
            public Pair<Boolean, byte[]> execute(byte[] serializedValue) {
                return new Pair<Boolean, byte[]>(Boolean.TRUE, serializedValue);
            }
        };
    }
}
