/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.persistence;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface Persistence {
    public void put(byte[] key, byte[] value);
    public void delete(byte[] key);
    public byte[] get(byte[] key);
    public Batch createBatch();
    public void writeBatch(Batch b);
    public StoreIterator iterator();
    public StoreIterator iterator(byte[] startKey);
}
