/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.persistence;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface Batch {
    public void put(byte[] key, byte[] value);
    public void delete(byte[] key);
    public void close();
}
