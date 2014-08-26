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

import java.util.Arrays;
import java.util.Map;
import com.google.common.base.Objects;
import java.util.UUID;
import org.javatuples.Triplet;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class PutObj {
    public static class Req extends DMMessage.Req {
        public final Triplet<ByteId, ByteId, ByteId> objId;
        public final byte[] objValue;
        public final Map<ByteId, Object> indexValue;

        public Req(UUID id, Triplet<ByteId, ByteId, ByteId> objId, byte[] value, Map<ByteId, Object> indexValue) {
            super(id);
            this.objId = objId;
            this.objValue = value;
            this.indexValue = indexValue;
        }

        @Override
        public String toString() {
            return "DM_PUT_GSONOBJ_REQ(" + id + ")";
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 17 * hash + Objects.hashCode(this.id);
            hash = 17 * hash + Objects.hashCode(this.objId);
            hash = 17 * hash + Arrays.hashCode(this.objValue);
            hash = 17 * hash + Objects.hashCode(this.indexValue);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Req other = (Req) obj;
            if (!Objects.equal(this.id, other.id)) {
                return false;
            }
            if (!Objects.equal(this.objId, other.objId)) {
                return false;
            }
            if (!Arrays.equals(this.objValue, other.objValue)) {
                return false;
            }
            if (!Objects.equal(this.indexValue, other.indexValue)) {
                return false;
            }
            return true;
        }
        
        
    }

    public static class Resp extends DMMessage.Resp {
        public final Triplet<ByteId, ByteId, ByteId> objId;
        
        public Resp(UUID id, DMMessage.ResponseCode opResult, Triplet<ByteId, ByteId, ByteId> objId) {
            super(id, opResult);
            this.objId = objId;
        }

        @Override
        public String toString() {
            return "DM_PUT_GSONOBJ_RESP(" + id + ")";
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 67 * hash + Objects.hashCode(this.id);
            hash = 67 * hash + Objects.hashCode(this.respCode);
            hash = 67 * hash + Objects.hashCode(this.objId);
            return hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final Resp other = (Resp) obj;
            if (!Objects.equal(this.id,other.id)) {
                return false;
            }
            if (this.respCode != other.respCode) {
                return false;
            }
            if (!Objects.equal(this.objId, other.objId)) {
                return false;
            }
            return true;
        }
        
        
    }
}