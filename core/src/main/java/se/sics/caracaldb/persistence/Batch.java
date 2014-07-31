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

import java.io.Closeable;
import se.sics.caracaldb.utils.ByteArrayRef;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface Batch extends Closeable {

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
    public void put(byte[] key, byte[] value, int version);

    /**
     * Replaces the whole multi-version blob at key with value
     *
     * @param key
     * @param value
     */
    public void replace(byte[] key, ByteArrayRef value);

    /**
     * Removes the mapping for this key in this version if present.
     * <p>
     * @param key
     */
    public void delete(byte[] key, int version);

    /**
     * Cleans up all versions before version of data at this key if exists
     *
     * @param key
     * @param version
     * @return the new data size
     */
    public int deleteVersions(byte[] key, int version);

    /**
     * Closes the batch and frees up it's resources.
     * 
     * This method does NOT commit the batch! That should have been done already.
     */
    public void close();
}
