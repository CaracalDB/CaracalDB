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

package se.sics.caracaldb.flow;

import se.sics.caracaldb.Address;
import se.sics.kompics.Request;

/**
 * The <code>RequestToSend</code> class.
 * 
 * @author Lars Kroll <lkroll@sics.se>
 * @version $$
 * 
 */
public class RequestToSend extends Request {
    private ClearToSend event;
    public final long hint;
    
    public RequestToSend() {
        hint = -1;
    }
    
    public RequestToSend(long hint) {
        this.hint = hint;
    }
    
    public void setEvent(ClearToSend cts) {
        this.event = cts;
    }
    
    public Address getDestination() {
        return this.event.getSource(); // invert because RTS is different direction than CTS
    }
    
    public Address getSource() {
        return this.event.getDestination(); // invert because RTS is different direction than CTS
    }
    
    public ClearToSend getEvent() {
        return this.event;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RequestToSend(");
        sb.append("hint: ");
        sb.append(hint);
        sb.append(", cts:\n");
        sb.append(event);
        sb.append("\n)");
        return sb.toString();
    }
}

