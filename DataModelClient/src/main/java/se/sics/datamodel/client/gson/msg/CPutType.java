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
package se.sics.datamodel.client.gson.msg;

import java.util.UUID;
import org.javatuples.Pair;
import se.sics.datamodel.DMSerializer;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.msg.PutType;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class CPutType {
    public final Pair<ByteId, ByteId> typeId;
    public final TypeInfo typeInfo;
    
    public CPutType(Pair<ByteId, ByteId> typeId, TypeInfo typeInfo) {
        this.typeId = typeId;
        this.typeInfo = typeInfo;
    }
    
    public PutType.Req getReq(UUID reqId) {
        return new PutType.Req(reqId, typeId, DMSerializer.serialize(typeInfo));
    }
}
