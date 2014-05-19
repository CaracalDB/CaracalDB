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
import java.util.Objects;
import java.util.UUID;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GetAllTypes {
    public static class Req extends DMMessage.Req {

        public final ByteId dbId;
        
        public Req(UUID id, ByteId dbId) {
            super(id);
            this.dbId = dbId;
        }
        
        @Override
        public String toString() {
            return "GET_ALLTYPES - request(" + id + ")";
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 73 * hash + Objects.hashCode(this.id);
            hash = 73 * hash + Objects.hashCode(this.dbId);
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
            if (!Objects.equals(this.dbId, other.dbId)) {
                return false;
            }
            return true;
        }
    }

    public static class Resp extends DMMessage.Resp {
        public final ByteId dbId;
        public final Map<String, ByteId> types;

        public Resp(UUID id, DMMessage.ResponseCode opResult, ByteId dbId, Map<String, ByteId> typesMap) {
            super(id, opResult);
            this.dbId = dbId;
            this.types = typesMap;
        }

        @Override
        public String toString() {
            return "GET_ALLTYPES - response(" + id + ")";
        }
        
        @Override
        public int hashCode() {
            int hash = 3;
            hash = 59 * hash + Objects.hashCode(this.id);
            hash = 59 * hash + Objects.hashCode(this.respCode);
            hash = 59 * hash + Objects.hashCode(this.dbId);
            hash = 59 * hash + Objects.hashCode(this.types);
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
            if (!Objects.equals(this.dbId, other.dbId)) {
                return false;
            }
            if (!Objects.equals(this.types, other.types)) {
                return false;
            }
            return true;
        }
    }
}
