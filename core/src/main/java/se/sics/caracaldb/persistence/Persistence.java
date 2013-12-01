/* 
 * This file is part of the CaracalDB distributed storage system.
 *
 * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
 * Copyright (C) 2009 Royal Institute of Technology (KTH)
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package se.sics.caracaldb.persistence;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface Persistence {

    /**
     * Associates the specified value with the specified key in this map. If the
     * map previously contained a mapping for the key, the old value is
     * replaced.
     * <p>
     * Do not write null as key or value! Use empty arrays instead.
     * <p>
     * @param key
     * @param value
     */
    public void put(byte[] key, byte[] value);

    /**
     * Removes the mapping for this key from this TreeMap if present.
     * <p>
     * @param key
     */
    public void delete(byte[] key);

    /**
     * Returns the value to which the specified key is mapped, or null if this
     * map contains no mapping for the key.
     * <p>
     * @param key
     * @return
     */
    public byte[] get(byte[] key);

    /**
     * Prepares a write batch (allocate native memory in case of native
     * databases)
     * <p>
     * @return
     */
    public Batch createBatch();

    /**
     * Write a previously created (by createBatch()) batch to the database
     * atomically.
     * <p>
     * @param b
     */
    public void writeBatch(Batch b);

    /**
     * Returns an object to iterate over the store from the beginning.
     * <p>
     * @return
     */
    public StoreIterator iterator();

    /**
     * Returns an object to iterate over the store from the start key
     * (inclusive).
     * <p>
     * @param startKey
     * @return
     */
    public StoreIterator iterator(byte[] startKey);
}
