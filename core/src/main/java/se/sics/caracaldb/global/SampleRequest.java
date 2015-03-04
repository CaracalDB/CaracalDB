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

import com.google.common.collect.ImmutableSet;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.Transport;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class SampleRequest extends Message {

    public final int n;
    public final boolean schemas;
    public final boolean lut;
    public final long lutversion;

    public SampleRequest(Address src, Address dest, int n, boolean schema, boolean lut, long lutversion) {
        super(src, dest, src, Transport.TCP);
        this.n = n;
        this.schemas = schema;
        this.lut = lut;
        this.lutversion = lutversion;
    }

    public Sample reply(ImmutableSet<Address> nodes, SchemaData schemaData) {
        if (schemas) {
            return new Sample(this.getDestination(), this.getSource(), nodes, schemaData.serialise());
        } else {
            return new Sample(this.getDestination(), this.getSource(), nodes, null);
        }
    }
}
