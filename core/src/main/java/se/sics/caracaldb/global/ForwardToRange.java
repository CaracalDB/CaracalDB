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

import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.kompics.Event;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Message;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ForwardToRange extends Event {

    private final RangeQuery.Request rangeReq;
    public final Address src;
    public final RangeQuery.Type execType;
    public final KeyRange range;

    public ForwardToRange(RangeQuery.Request rangeReq, KeyRange range, Address src) {
        this.rangeReq = rangeReq;
        this.src = src;
        this.execType = rangeReq.execType;
        this.range = range;
    }

    public Message getSubRangeMessage(KeyRange subRange, Address dst) {
        return new CaracalMsg(src, dst, rangeReq.subRange(subRange));
    }
}
