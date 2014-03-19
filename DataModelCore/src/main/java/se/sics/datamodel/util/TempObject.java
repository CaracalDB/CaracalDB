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
package se.sics.datamodel.util;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
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
    public final Value objValue;

    public TempObject(TempTypeInfo typeInfo, ByteId objId) {
        this.typeInfo = typeInfo;
        this.objId = objId;
        this.objValue = new Value();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("objId:\n ").append(objId.toString()).append("\n");
        sb.append("typeInfo:\n").append(typeInfo.toString()).append("\n");
        sb.append("objValue:\n").append(objValue.toString()).append("\n");
        return sb.toString();
    }
    
    public Map<ByteId, Object> getIndexValue() {
        Map<ByteId, Object> indexValues = new TreeMap<ByteId, Object>();
        for(TempTypeInfo.TempFieldInfo fi : typeInfo.fieldMap.values()) {
            if(fi.indexed) {
                indexValues.put(fi.fieldId, objValue.fieldMap.get(fi.fieldId));
            }
        }
        return indexValues;
    }

    public static class Value {

        public final Map<ByteId, Object> fieldMap;

        private Value() {
            this.fieldMap = new TreeMap<ByteId, Object>();
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for(Map.Entry<ByteId, Object> e : fieldMap.entrySet()) {
                sb.append("fieldId: ").append(e.getKey().toString()).append("\n");
            }
            return sb.toString();
        }

        public static class GsonTypeAdapter extends TypeAdapter<Value> {

            @Override
            public void write(JsonWriter writer, Value t) throws IOException {
                Gson gson = GsonHelper.getGson();

                writer.beginObject();
                writer.name("fieldMap");
                writer.beginArray();
                for (Map.Entry<ByteId, Object> e : t.fieldMap.entrySet()) {
                    writer.beginObject();
                    writer.name("fieldId");
                    gson.toJson(gson.toJsonTree(e.getKey()), writer);
                    writer.name("fieldValue");
                    gson.toJson(gson.toJsonTree(e.getValue()), writer);
                    writer.endObject();
                }
                writer.endArray();
                writer.endObject();
            }

            @Override
            public Value read(JsonReader reader) throws IOException {
                throw new UnsupportedOperationException("Should not read as Value but as ValueHolder");
            }
        }
    }

    public static class ValueHolder {

        public final Map<ByteId, JsonElement> fieldMap;

        private ValueHolder() {
            this.fieldMap = new TreeMap<ByteId, JsonElement>();
        }

        public Value getValue(TempTypeInfo typeInfo) {
            if (typeInfo == null) {
                return null;
            }

            if (typeInfo.fieldMap.size() != fieldMap.size()) {
                return null;
            }

            Gson gson = GsonHelper.getGson();
            Value objValue = new Value();
            for (TempTypeInfo.TempFieldInfo fi : typeInfo.fieldMap.values()) {
                JsonElement je = fieldMap.get(fi.fieldId);
                if (je == null) {
                    return null;
                }

                objValue.fieldMap.put(fi.fieldId, gson.fromJson(je, getClass(fi.fieldType)));
            }

            return objValue;
        }

        private Class<?> getClass(FieldInfo.FieldType fieldType) {
            if (fieldType == FieldInfo.FieldType.BOOLEAN) {
                return Boolean.class;
            } else if (fieldType == FieldInfo.FieldType.FLOAT) {
                return Float.class;
            } else if (fieldType == FieldInfo.FieldType.INTEGER) {
                return Integer.class;
            } else if (fieldType == FieldInfo.FieldType.STRING) {
                return String.class;
            } else {
                return null;
            }
        }

        public static class GsonTypeAdapter extends TypeAdapter<ValueHolder> {

            @Override
            public void write(JsonWriter writer, ValueHolder t) throws IOException {
                throw new UnsupportedOperationException("Should not write as ValueHolder, but as Value");
            }

            @Override
            public ValueHolder read(JsonReader reader) throws IOException {
                Gson gson = GsonHelper.getGson();
                ValueHolder objValue = new ValueHolder();

                JsonToken token = reader.peek();
                if (token == JsonToken.NULL) {
                    reader.nextNull();
                    return null;
                }
                if (token != JsonToken.BEGIN_OBJECT) {
                    return null;
                }
                reader.beginObject();

                if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("fieldMap")) {
                    return null;
                }
                if (reader.peek() != JsonToken.BEGIN_ARRAY) {
                    return null;
                }
                reader.beginArray();

                while (reader.peek() != JsonToken.END_ARRAY) {
                    if (reader.peek() != JsonToken.BEGIN_OBJECT) {
                        return null;
                    }
                    reader.beginObject();

                    if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("fieldId")) {
                        return null;
                    }
                    ByteId fieldId = gson.fromJson(reader, ByteId.class);

                    if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("fieldValue")) {
                        return null;
                    }
                    JsonElement je = gson.fromJson(reader, JsonElement.class);

                    objValue.fieldMap.put(fieldId, je);

                    if (reader.peek() != JsonToken.END_OBJECT) {
                        return null;
                    }
                    reader.endObject();
                }

                reader.endArray();

                if (reader.peek() != JsonToken.END_OBJECT) {
                    return null;
                }

                reader.endObject();
                return objValue;
            }
        }
    }
}
