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
package se.sics.caracaldb.datamodel.msg;

import se.sics.caracaldb.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GetObj {

    public static class Req extends DMMessage.Req {

        public final ByteId dbId;
        public final ByteId typeId;
        public final ByteId objId;

        public Req(long id, ByteId dbId, ByteId typeId, ByteId objId) {
            super(id);
            this.dbId = dbId;
            this.typeId = typeId;
            this.objId = objId;
        }

        @Override
        public String toString() {
            return "DM_GET_GSONOBJ_REQ(" + id + ")";
        }
    }

    public static class Resp extends DMMessage.Resp {
        public final ByteId dbId;
        public final ByteId typeId;
        public final ByteId objId;
        public final byte[] value;

        public Resp(long id, DMMessage.ResponseCode opResult, ByteId dbId, ByteId typeId, ByteId objId, byte[] value) {
            super(id, opResult);
            this.dbId = dbId;
            this.typeId = typeId;
            this.objId = objId;
            this.value = value;
        }

        @Override
        public String toString() {
            return "DM_GET_GSONOBJ_RESP(" + id + ")";
        }
    }
}
