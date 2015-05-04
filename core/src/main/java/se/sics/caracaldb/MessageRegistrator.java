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

import se.sics.caracaldb.datatransfer.TransferMessage;
import se.sics.caracaldb.flow.CollectorDescriptor;
import se.sics.caracaldb.global.ForwardMessage;
import se.sics.caracaldb.global.LookupMessage;
import se.sics.caracaldb.global.Schema;
import se.sics.caracaldb.operations.CaracalMsg;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.store.MultiOp;
import se.sics.caracaldb.system.StartVNode;
import se.sics.caracaldb.system.StopVNode;
import se.sics.kompics.network.netty.serialization.Serializers;

/**
 * Register all necessary messages here so they get done before Grizzly is
 * loaded
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public abstract class MessageRegistrator {
    
    public static final int STREAM_MAX_MSG_SIZE = 64000;
    public static final int UDP_MAX_MSG_SIZE = 1500;

    public static void register() {
        Serializers.register(CoreSerializer.SYSTEM.instance, "systemS");
        Serializers.register(StartVNode.class, "systemS");
        Serializers.register(StopVNode.class, "systemS");
        //
        Serializers.register(CoreSerializer.OP.instance, "opS");
        Serializers.register(CaracalMsg.class, "opS");
        Serializers.register(CaracalOp.class, "opS");
        //
        Serializers.register(CoreSerializer.LOOKUP.instance, "lookupS");
        Serializers.register(ForwardMessage.class, "lookupS");
        Serializers.register(LookupMessage.class, "lookupS");
        Serializers.register(Schema.Req.class, "lookupS");
        Serializers.register(Schema.Response.class, "lookupS");
        //
        Serializers.register(CoreSerializer.COND.instance, "condS");
        Serializers.register(MultiOp.Condition.class, "condS");
        //
        Serializers.register(CoreSerializer.DTS.instance, "transferS");
        Serializers.register(TransferMessage.class, "transferS");
        //
        Serializers.register(CoreSerializer.CDS.instance, "collectorS");
        Serializers.register(CollectorDescriptor.class, "collectorS");
    }
}
