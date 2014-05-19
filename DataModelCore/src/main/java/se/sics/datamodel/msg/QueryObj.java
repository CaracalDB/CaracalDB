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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.javatuples.Pair;
import se.sics.caracaldb.store.Limit.LimitTracker;
import se.sics.datamodel.QueryType;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class QueryObj {
    public static class Req extends DMMessage.Req {
        public final Pair<ByteId, ByteId> typeId; 
        public final ByteId indexId;
        public final QueryType indexVal;
        public final LimitTracker limit;
        
        public Req(UUID id, Pair<ByteId, ByteId> typeId, ByteId indexId, QueryType indexVal, LimitTracker limit) {
            super(id);
            this.typeId = typeId;
            this.indexId = indexId;
            this.indexVal = indexVal;
            this.limit = limit;
        }
        
        @Override
        public String toString() {
            return "DM_QUERY_OBJ_REQ(" + id + ")";
        }

        @Override
        public int hashCode() {
            int hash = 7;
            hash = 59 * hash + Objects.hashCode(this.id);
            hash = 59 * hash + Objects.hashCode(this.typeId);
            hash = 59 * hash + Objects.hashCode(this.indexId);
            hash = 59 * hash + Objects.hashCode(this.indexVal);
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
            if (!Objects.equals(this.id, other.id)) {
                return false;
            }
            if (!Objects.equals(this.typeId, other.typeId)) {
                return false;
            }
            if (!Objects.equals(this.indexId, other.indexId)) {
                return false;
            }
            if (!Objects.equals(this.indexVal, other.indexVal)) {
                return false;
            }
            return true;
        }
    }
    
    public static class Resp extends DMMessage.Resp {
        public final Pair<ByteId, ByteId> typeId; 
        public final Map<ByteId, ByteBuffer> objs;
        
        public Resp(UUID id, DMMessage.ResponseCode respCode, Pair<ByteId, ByteId> typeId, Map<ByteId, ByteBuffer> objs) {
            super(id, respCode);
            this.typeId = typeId;
            this.objs = objs;
        }
        
        @Override
        public String toString() {
            return "DM_QUERY_OBJ_RESP(" + id + ")";
        }

        @Override
        public int hashCode() {
            int hash = 5;
            hash = 11 * hash + Objects.hashCode(this.id);
            hash = 11 * hash + Objects.hashCode(this.respCode);
            hash = 11 * hash + Objects.hashCode(this.typeId);
            hash = 11 * hash + Objects.hashCode(this.objs);
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
            if (!Objects.equals(this.id, other.id)) {
                return false;
            }
            if (this.respCode != other.respCode) {
                return false;
            }
            if (!Objects.equals(this.typeId, other.typeId)) {
                return false;
            }
            if (!Objects.equals(this.objs, other.objs)) {
                return false;
            }
            return true;
        }
    }
}