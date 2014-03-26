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
package se.sics.datamodel;

import se.sics.datamodel.util.ByteId;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 * TODO turn map into ImmutableMap as TypeInfo should be immutable
 */
public class TypeInfo {

    public final String name;
    private final Map<ByteId, FieldInfo> fieldMap;

    private TypeInfo(String name, Map<ByteId, FieldInfo> fieldMap) {
        this.name = name;
        this.fieldMap = fieldMap;
    }
    
    public FieldInfo get(ByteId fieldId) {
        return fieldMap.get(fieldId);
    }
    
    public Set<Entry<ByteId, FieldInfo>> entrySet() {
        return fieldMap.entrySet();
    }
    
    public int size() {
        return fieldMap.size();
    }

    @Override
    public String toString() {
        return DMSerializer.asString(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final TypeInfo other = (TypeInfo) obj;
        if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
            return false;
        }
        if (this.fieldMap != other.fieldMap && (this.fieldMap == null || !this.fieldMap.equals(other.fieldMap))) {
            return false;
        }
        return true;
    }

    
    public static class Builder {

        private final String name;
        private final Map<ByteId, FieldInfo> fiMap;

        public Builder(String name) {
            this.name = name;
            this.fiMap = new TreeMap<ByteId, FieldInfo>();
        }

        public Builder put(ByteId fieldId, FieldInfo fieldInfo) {
            fiMap.put(fieldId, fieldInfo);
            return this;
        }

        public TypeInfo build() {
            return new TypeInfo(name, new TreeMap<ByteId, FieldInfo>(fiMap));
        }
    }
    
    public static class InconsistencyException extends Exception {
        public InconsistencyException() {
            super();
        }
        public InconsistencyException(String msg) {
            super(msg);
        }
    }
}