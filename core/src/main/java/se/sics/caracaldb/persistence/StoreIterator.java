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
import java.util.SortedMap;
import com.larskroll.common.ByteArrayRef;

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
    public byte[] peekKey();
    public ByteArrayRef peekValue();
    public SortedMap<Integer, ByteArrayRef> peekAllValues();
    public byte[] peekRaw();
}
