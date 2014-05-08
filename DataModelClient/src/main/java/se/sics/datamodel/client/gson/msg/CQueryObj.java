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

import org.javatuples.Pair;
import se.sics.caracaldb.store.Limit;
import se.sics.datamodel.FieldType;
import se.sics.datamodel.QueryType;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.client.gson.util.QueryTypeHolder;
import se.sics.datamodel.msg.GetType;
import se.sics.datamodel.msg.QueryObj;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class CQueryObj {

    public final Pair<ByteId, ByteId> typeId;
    public final ByteId indexId;
    public final QueryTypeHolder indexVal;

    public CQueryObj(Pair<ByteId, ByteId> typeId, ByteId indexId, QueryTypeHolder indexVal) {
        this.typeId = typeId;
        this.indexId = indexId;
        this.indexVal = indexVal;
    }

    public GetType.Req getTypeReq(long id) {
        return new GetType.Req(id, typeId);
    }

    public QueryObj.Req getQueryReq(long id, TypeInfo typeInfo) {
        FieldType indexedField = typeInfo.get(indexId).type;
        QueryType oIndexVal = indexVal.getQueryType(indexedField);
        return new QueryObj.Req(id, typeId, indexId, oIndexVal, Limit.noLimit());
    }
}