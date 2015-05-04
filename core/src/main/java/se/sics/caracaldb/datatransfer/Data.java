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
package se.sics.caracaldb.datatransfer;

import com.larskroll.common.DataRef;
import se.sics.caracaldb.flow.CollectorDescriptor;
import se.sics.kompics.KompicsEvent;

/**
 *
 * @author lkroll
 */
public abstract class Data {

    public static class Request implements KompicsEvent {

        public final long size;

        public Request(long size) {
            this.size = size;
        }
    }

    public static class Reference implements KompicsEvent {

        public final DataRef data;
        public final CollectorDescriptor collector;
        public final boolean isFinal;

        public Reference(DataRef data, CollectorDescriptor collector, boolean isFinal) {
            this.data = data;
            this.collector = collector;
            this.isFinal = isFinal;
        }
    }

    public static class Requirements implements KompicsEvent {
        public final long minQuota;
        
        public Requirements(long minQuota) {
            this.minQuota = minQuota;
        }
    }
}
