/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.sics.caracaldb.persistence;

import java.io.Closeable;
import java.util.Map.Entry;

/**
 * @author Lars Kroll <lkroll@sics.se>
 * @author Alex Ormenisan <aaor@sics.se>
 * 
 * normal method order invocation is hasNext->peek->next
 * any invocation of next followed directly by a peek can throw NullPointerException 
 * if there is no item to peek at
 */
public interface StoreIterator extends Closeable {
    /**
     * @return true if the iterator can peek at next item
     */
    public boolean hasNext();
    /**
     * moves the iterator to the next item. Use peek to inspect key/value of this item
     */
    public void next();
    public Entry<byte[], byte[]> peekNext();
    public byte[] peekKey();
    public byte[] peekValue();
}
