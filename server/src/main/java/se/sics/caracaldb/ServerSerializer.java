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
package se.sics.caracaldb;

import se.sics.caracaldb.bootstrap.BootstrapSerializer;
import se.sics.caracaldb.global.MaintenanceSerializer;
import se.sics.caracaldb.replication.linearisable.XnginSerializer;
import se.sics.caracaldb.vhostfd.FDSerializer;
import se.sics.kompics.network.netty.serialization.Serializer;

/**
 *
 * @author lkroll
 */
public class ServerSerializer<S extends Serializer> {

    public static final ServerSerializer<FDSerializer> FD = new ServerSerializer(200, new FDSerializer());
    public static final ServerSerializer<MaintenanceSerializer> GLOBAL = new ServerSerializer(201, new MaintenanceSerializer());
    public static final ServerSerializer<XnginSerializer> XNGIN = new ServerSerializer(202, new XnginSerializer());
    public static final ServerSerializer<BootstrapSerializer> BOOT = new ServerSerializer(203, new BootstrapSerializer());
    
    public final int id;
    public final S instance;

    private ServerSerializer(int id, S instance) {
        this.id = id;
        this.instance = instance;
    }
}
