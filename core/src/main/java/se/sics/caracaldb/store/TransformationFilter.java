/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.store;

import org.javatuples.Pair;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public interface TransformationFilter {

    /**
     * @param value serialized item
     * @return <filtered,value> where filtered is true if the value is supposed
     * to be filtered out and false otherwise and value is the transformed final
     * value as byte array
     */
    public Pair<Boolean, byte[]> execute(byte[] serializedValue);
}