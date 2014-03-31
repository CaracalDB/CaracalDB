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
package se.sics.datamodel.client.msg;

import org.javatuples.Triplet;
import se.sics.datamodel.IndexHelper;
import se.sics.datamodel.ObjectValue;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.ValueHolder;
import se.sics.datamodel.client.CDMSerializer;
import se.sics.datamodel.msg.GetType;
import se.sics.datamodel.msg.PutObj;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class CPutObj {
    public final Triplet<ByteId, ByteId, ByteId> objId;
    public final ValueHolder vh;
    
    public CPutObj(Triplet<ByteId, ByteId, ByteId> objId, ValueHolder vh) {
        this.objId = objId;
        this.vh = vh;
    }
    
    public GetType.Req getTypeReq(long reqId) {
        return new GetType.Req(reqId, objId.removeFrom2());
    }
    
    public PutObj.Req putObjReq(long reqId, TypeInfo typeInfo) {
        ObjectValue ov;
        try {
            ov = vh.getObjectValue(typeInfo);
            return new PutObj.Req(reqId, objId, CDMSerializer.serialize(ov), IndexHelper.getIndexes(typeInfo, ov));
        } catch (TypeInfo.InconsistencyException ex) {
            throw new RuntimeException(ex);
        }
    }
}