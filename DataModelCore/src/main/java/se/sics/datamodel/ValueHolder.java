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

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import se.sics.datamodel.FieldInfo;
import se.sics.datamodel.FieldType;
import se.sics.datamodel.ObjectValue;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.TypeInfo.InconsistencyException;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.util.gson.GsonHelper;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ValueHolder {

    private final Map<ByteId, JsonElement> fieldMap;

    private ValueHolder(Map<ByteId, JsonElement> fieldMap) {
        this.fieldMap = fieldMap;
    }

    private Object get(ByteId fieldId) {
        return fieldMap.get(fieldId);
    }

    private Set<Entry<ByteId, JsonElement>> entrySet() {
        return fieldMap.entrySet();
    }

    public ObjectValue getObjectValue(TypeInfo ti) throws InconsistencyException {
        ObjectValue.Builder ovBuilder = new ObjectValue.Builder();
        
        Gson gson = GsonHelper.getGson();
        for (Entry<ByteId, JsonElement> e : fieldMap.entrySet()) {
            FieldInfo fi = ti.get(e.getKey());
            if (fi == null) {
                throw new InconsistencyException();
            }
            Object fieldVal = gson.fromJson(e.getValue(), FieldType.getFieldClass(fi.type));
            ovBuilder.put(e.getKey(), fieldVal);
        }
        return ovBuilder.build();
    }
    
    public static class Builder {

        private final Map<ByteId, JsonElement> fieldMap;

        public Builder() {
            this.fieldMap = new TreeMap<ByteId, JsonElement>();
        }

        public void put(ByteId fieldId, JsonElement fieldVal) {
            fieldMap.put(fieldId, fieldVal);
        }

        public ValueHolder build() {
            return new ValueHolder(new TreeMap<ByteId, JsonElement>(fieldMap));
        }
    }
}
