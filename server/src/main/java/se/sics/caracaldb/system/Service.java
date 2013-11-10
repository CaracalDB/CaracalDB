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

import com.google.common.collect.ComparisonChain;
import se.sics.kompics.Component;
import se.sics.kompics.PortType;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class Service implements Comparable<Service> {
    public final String name;
    public final Class<? extends PortType> type;
    public final Component provider;
    
    public Service(String name, Class<? extends PortType> type, Component provider) {
        this.name = name;
        this.type = type;
        this.provider = provider;
    }

    @Override
    public int compareTo(Service that) {
        return ComparisonChain.start()
         .compare(this.type.getCanonicalName(), that.type.getCanonicalName())
         .compare(this.name, that.name)
         .result();
    }
}
