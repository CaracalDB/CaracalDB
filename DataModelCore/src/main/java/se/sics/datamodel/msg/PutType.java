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
package se.sics.datamodel.msg;

import org.javatuples.Pair;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class PutType {

    public static class Req extends DMMessage.Req {

//        public final Pair<ByteId,ByteId> typeId; //<dbId,typeId> //kryo is stupid and can' do tuples
        public final ByteId dbId;
        public final ByteId typeId;
        public final byte[] typeInfo;

        public Req(long id, Pair<ByteId, ByteId> typeId, byte[] typeInfo) {
            super(id);
            this.dbId = typeId.getValue0();
            this.typeId = typeId.getValue1();
            this.typeInfo = typeInfo;
        }

        @Override
        public String toString() {
            return "DM_PUT_TYPE_REQ(" + id + ")";
        }
    }

    public static class Resp extends DMMessage.Resp {
        public final ByteId dbId;
        public final ByteId typeId;
        
        public Resp(long id, DMMessage.ResponseCode opResult, Pair<ByteId, ByteId> typeId) {
            super(id, opResult);
            this.dbId = typeId.getValue0();
            this.typeId = typeId.getValue1();
        }

        @Override
        public String toString() {
            return "DM_PUT_TYPE_RESP(" + id + ")";
        }
    }
}
