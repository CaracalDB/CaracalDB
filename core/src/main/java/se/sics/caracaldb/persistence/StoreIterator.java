/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.persistence;

import java.util.Map.Entry;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface StoreIterator {
    public boolean hasNext();
    public void next();
    public Entry<byte[], byte[]> peekNext();
    public byte[] peekKey();
    public byte[] peekValue();
    public void close();
}
