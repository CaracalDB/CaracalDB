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

import java.util.Map;
import org.javatuples.Triplet;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class PutObj {
    public static class Req extends DMMessage.Req {
        public final Triplet<ByteId, ByteId, ByteId> objId; //<dbId, typeId, objId>
        public final byte[] objValue;
        public final Map<ByteId, Object> indexValue;

        public Req(long id, Triplet<ByteId, ByteId, ByteId> objId, byte[] value, Map<ByteId, Object> indexValue) {
            super(id);
            this.objId = objId;
            this.objValue = value;
            this.indexValue = indexValue;
        }

        @Override
        public String toString() {
            return "DM_PUT_GSONOBJ_REQ(" + id + ")";
        }
    }

    public static class Resp extends DMMessage.Resp {
        public final Triplet<ByteId, ByteId, ByteId> objId;
        
        public Resp(long id, Triplet<ByteId, ByteId, ByteId> objId, DMMessage.ResponseCode opResult) {
            super(id, opResult);
            this.objId = objId;
        }

        @Override
        public String toString() {
            return "DM_PUT_GSONOBJ_RESP(" + id + ")";
        }
    }
}