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

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */


public class ObjectValue {
    private final Map<ByteId, Object> fieldMap;
    
    private ObjectValue(Map<ByteId, Object> fieldMap) {
        this.fieldMap = fieldMap;
    }
    
    public Object get(ByteId fieldId) {
        return fieldMap.get(fieldId);
    }
    
    public Set<Entry<ByteId, Object>> entrySet() {
        return fieldMap.entrySet();
    }
    
    public int size() {
        return fieldMap.size();
    }

    @Override
    public int hashCode() {
        return fieldMap.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final ObjectValue other = (ObjectValue) obj;
        if (this.fieldMap != other.fieldMap && (this.fieldMap == null || !this.fieldMap.equals(other.fieldMap))) {
            return false;
        }
        return true;
    }
    
    public static class Builder {
        private final Map<ByteId, Object> fieldMap;
        
        public Builder() {
            this.fieldMap = new TreeMap<ByteId, Object>();
        }
        
        public Builder put(ByteId fieldId, Object fieldVal) {
            fieldMap.put(fieldId, fieldVal);
            return this;
        }
        
        public ObjectValue build() {
            return new ObjectValue(new TreeMap<ByteId, Object>(fieldMap));
        }
    }
}
