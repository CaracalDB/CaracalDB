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
package se.sics.caracaldb.vhostfd;

import se.sics.caracaldb.Address;
import se.sics.kompics.Init;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class VEPFDInit extends Init {
    public final long minRto, livePingInterval, deadPingInterval, pongTimeoutIncrement;
    public final Address self;
    
    public VEPFDInit(Address self, long minRto, long livePingInterval, long deadPingInterval, long pongTimeoutIncrement) {
        this.self = self;
        this.minRto = minRto;
        this.livePingInterval = livePingInterval;
        this.deadPingInterval = deadPingInterval;
        this.pongTimeoutIncrement = pongTimeoutIncrement;
    }
}
