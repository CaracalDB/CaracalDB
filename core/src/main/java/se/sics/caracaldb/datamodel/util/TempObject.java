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
package se.sics.caracaldb.datamodel.util;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class TempObject {

    public final TempTypeInfo typeInfo;
    public final ByteId objId;
    public final TempObject.Value objValue;
    private final TempObject.TempValue objTempValue;
    
    public TempObject(TempTypeInfo typeInfo, ByteId objId) {
        this.typeInfo = typeInfo;
        this.objId = objId;
        this.objValue = new Value();
        this.objTempValue = new TempValue();
    }

    public static class Value {

        public final Map<ByteId, Object> fieldMap;

        private Value() {
            this.fieldMap = new TreeMap<ByteId, Object>();
        }
        
        public Value addField(ByteId fieldId, Object field) {
            fieldMap.put(fieldId, field);
            return this;
        }
    }
    
    public static class TempValue {
        public final Map<ByteId, JsonElement> fieldMap;
        
        private TempValue() {
            this.fieldMap = new TreeMap<ByteId, JsonElement>();
        }
        
        public TempValue addField(ByteId fieldId, JsonElement field) {
            fieldMap.put(fieldId, field);
            return this;
        }
    }

    public static class GsonTypeAdapter extends TypeAdapter<TempObject> {

        @Override
        public void write(JsonWriter writer, TempObject t) throws IOException {
            Gson gson = GsonHelper.getGson();

            writer.beginObject();
            writer.beginArray();
            for (Map.Entry<ByteId, Object> e : t.objValue.fieldMap.entrySet()) {
                gson.toJson(gson.toJsonTree(e.getValue()), writer);
            }
            writer.endArray();
            writer.endObject();
        }

        @Override
        public TempObject read(JsonReader reader) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

    }
}
