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
package se.sics.caracaldb.global;

import java.util.UUID;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Msg;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public interface Forwardable<T extends Msg> {
    /**
     * Return a message with desired contents and with the given address as dest.
     * 
     * @param src Address where message is from (now, original sender is kept in orig)
     * @param dest Address where message is forwarded to
     * @return Message to be forwarded
     */
    public T insertDestination(Address src, Address dest);
    
    /**
     * Return a unique ID for this Forwardable.
     * 
     * This is used to avoid endless redirect loops.
     * 
     * @return 
     */
    public UUID getId();
}
