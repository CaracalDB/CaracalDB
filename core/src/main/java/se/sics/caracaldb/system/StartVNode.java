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
package se.sics.caracaldb.system;

import se.sics.caracaldb.Address;
import se.sics.caracaldb.global.SchemaData.SingleSchema;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class StartVNode extends SystemMsg {
    
    public final byte[] nodeId;
    public final SingleSchema schema;
    
    public StartVNode(Address from, Address to, byte[] nodeId, SingleSchema schema) {
        super(from, to);
        
        this.nodeId = nodeId;
        this.schema = schema;
    }
    
}
