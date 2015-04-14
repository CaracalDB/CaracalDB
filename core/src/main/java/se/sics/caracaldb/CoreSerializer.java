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

import se.sics.caracaldb.fd.SimpleFDSerializer;
import se.sics.caracaldb.flow.FlowMessageSerializer;
import se.sics.caracaldb.global.LookupSerializer;
import se.sics.caracaldb.operations.ConditionSerializer;
import se.sics.caracaldb.operations.OperationSerializer;
import se.sics.caracaldb.paxos.PaxosSerializer;
import se.sics.caracaldb.replication.log.ValueSerializer;
import se.sics.caracaldb.system.SystemSerializer;
import se.sics.kompics.network.netty.serialization.Serializer;

/**
 *
 * @author lkroll
 */
public class CoreSerializer<S extends Serializer> {
    
    public static final CoreSerializer<FlowMessageSerializer> FLOW = new CoreSerializer<FlowMessageSerializer>(100, new FlowMessageSerializer());
    public static final CoreSerializer<PaxosSerializer> PAXOS = new CoreSerializer<PaxosSerializer>(101, new PaxosSerializer());
    public static final CoreSerializer<ValueSerializer> VALUE = new CoreSerializer<ValueSerializer>(102, new ValueSerializer());
    public static final CoreSerializer<SystemSerializer> SYSTEM = new CoreSerializer<SystemSerializer>(103, new SystemSerializer());
    public static final CoreSerializer<OperationSerializer> OP = new CoreSerializer<OperationSerializer>(104, new OperationSerializer());
    public static final CoreSerializer<LookupSerializer> LOOKUP = new CoreSerializer<LookupSerializer>(105, new LookupSerializer());
    public static final CoreSerializer<ConditionSerializer> COND = new CoreSerializer<ConditionSerializer>(106, new ConditionSerializer());
    public static final CoreSerializer<SimpleFDSerializer> SFD = new CoreSerializer<SimpleFDSerializer>(107, new SimpleFDSerializer());
    
    public final int id;
    public final S instance;
    
    private CoreSerializer(int id, S instance) {
        this.id = id;
        this.instance = instance;
    }
}
