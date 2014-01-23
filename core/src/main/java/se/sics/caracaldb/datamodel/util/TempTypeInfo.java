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
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class TempTypeInfo {

    public final String typeName;
    public final ByteId dbId;
    public final ByteId typeId;
    public final Map<ByteId, TempFieldInfo> fieldMap;
    public final ByteIdFactory bif;

    public TempTypeInfo(String typeName, ByteId dbId, ByteId typeId) {
        this.typeName = typeName;
        this.dbId = dbId;
        this.typeId = typeId;
        this.fieldMap = new TreeMap<ByteId, TempFieldInfo>();
        this.bif = new ByteIdFactory();
    }
    
    public TempTypeInfo addField(String fieldName, FieldInfo.FieldType fieldType, boolean indexed) {
        TempFieldInfo fi = new TempFieldInfo(bif.nextId(), fieldName, fieldType, indexed);
        fieldMap.put(fi.fieldId, fi);
        return this;
    }
    private TempTypeInfo addField(TempFieldInfo field) {
        fieldMap.put(field.fieldId, field);
        return this;
    }

    public void deserializeField(byte[] bFieldInfo) throws UnsupportedEncodingException {
        deserializeField(new String(bFieldInfo, "UTF8"));
    }

    public void deserializeField(String sFieldInfo) {
        Gson gson = new Gson();
        TempFieldInfo fieldInfo = gson.fromJson(sFieldInfo, TempFieldInfo.class);
        fieldMap.put(fieldInfo.fieldId, fieldInfo);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("typeName: ").append(typeName).append("\n");
        sb.append("dbId: ").append(dbId).append("\n");
        sb.append("typeId: ").append(typeId).append("\n");
        for (Map.Entry<ByteId, TempFieldInfo> e : fieldMap.entrySet()) {
            sb.append("field: ").append(e.getValue()).append("\n");
        }
        return sb.toString();
    }

    public static byte[] serializeField(TempFieldInfo fieldInfo) throws UnsupportedEncodingException {
        Gson gson = new Gson();
        String s = gson.toJson(fieldInfo);
        return s.getBytes("UTF-8");
    }

    public static class TempFieldInfo {

        private ByteId fieldId;
        private String fieldName;
        private FieldInfo.FieldType fieldType;
        private boolean indexed;

        TempFieldInfo() {
        }

        TempFieldInfo(ByteId fieldId, String fieldName, FieldInfo.FieldType fieldType, boolean indexed) {
            this.fieldId = fieldId;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.indexed = indexed;
        }

        @Override
        public String toString() {
            return "fieldId: " + fieldId + " fieldType: " + fieldType + " fieldName: " + fieldName + " indexed: " + indexed;
        }

        public static class GsonTypeAdapter extends TypeAdapter<TempFieldInfo> {

            @Override
            public void write(JsonWriter writer, TempFieldInfo t) throws IOException {
                Gson gson = GsonHelper.getGson();

                writer.beginObject();
                writer.name("fieldId");
                gson.toJson(gson.toJsonTree(t.fieldId), writer);
                writer.name("fieldName");
                gson.toJson(t.fieldName);
                writer.name("fieldType");
                writer.value(t.fieldType.toString());
                writer.name("indexed");
                writer.value(t.indexed);
                writer.endObject();
            }

            @Override
            public TempFieldInfo read(JsonReader reader) throws IOException {
                Gson gson = GsonHelper.getGson();

                JsonToken token = reader.peek();
                if (token == JsonToken.NULL) {
                    reader.nextNull();
                    return null;
                }
                if (token != JsonToken.BEGIN_OBJECT) {
                    return null;
                }

                reader.beginObject();

                ByteId fieldId;
                String fieldName;
                FieldInfo.FieldType fieldType;
                boolean indexed;
                
                if (reader.peek() != JsonToken.NAME) {
                    return null;
                }
                if (!reader.nextName().equals("fieldId")) {
                    return null;
                }

                fieldId = gson.fromJson(reader, ByteId.class);

                if (reader.peek() != JsonToken.NAME) {
                    return null;
                }
                if (!reader.nextName().equals("fieldName")) {
                    return null;
                }
                fieldName = reader.nextString();
                
                if (reader.peek() != JsonToken.NAME) {
                    return null;
                }
                if (!reader.nextName().equals("fieldType")) {
                    return null;
                }
                try {
                    fieldType = FieldInfo.FieldType.parseString(reader.nextString());
                } catch (TypeInfo.InconsistencyException ex) {
                    return null;
                }
                
                if (reader.peek() != JsonToken.NAME) {
                    return null;
                }
                if (!reader.nextName().equals("indexed")) {
                    return null;
                }
                indexed = Boolean.parseBoolean(reader.nextString());

                reader.endObject();
                return new TempFieldInfo(fieldId, fieldName, fieldType, indexed);
            }

        }
    }

    public static class GsonTypeAdapter extends TypeAdapter<TempTypeInfo> {

        public GsonTypeAdapter() {
        }

        @Override
        public void write(JsonWriter writer, TempTypeInfo t) throws IOException {
            Gson gson = GsonHelper.getGson();

            writer.beginObject();
            writer.name("typeName");
            writer.value(t.typeName);
            writer.name("dbId");
            gson.toJson(gson.toJsonTree(t.dbId), writer);
            writer.name("typeId");
            gson.toJson(gson.toJsonTree(t.typeId), writer);
            writer.name("fieldMap");
            writer.beginArray();
            for(Map.Entry<ByteId, TempFieldInfo> e : t.fieldMap.entrySet()) {
                gson.toJson(gson.toJsonTree(e.getValue()), writer);
            }
            writer.endArray();
            writer.endObject();
        }

        @Override
        public TempTypeInfo read(JsonReader reader) throws IOException {
            Gson gson = GsonHelper.getGson();

            JsonToken token = reader.peek();
            if (token == JsonToken.NULL) {
                reader.nextNull();
                return null;
            }
            if (token != JsonToken.BEGIN_OBJECT) {
                return null;
            }

            reader.beginObject();

            String typeName;
            ByteId dbId, typeId;

            if (reader.peek() != JsonToken.NAME) {
                return null;
            }
            if (!reader.nextName().equals("typeName")) {
                return null;
            }
            
            typeName = gson.fromJson(reader, String.class);
            
            if (reader.peek() != JsonToken.NAME) {
                return null;
            }
            if (!reader.nextName().equals("dbId")) {
                return null;
            }

            dbId = gson.fromJson(reader, ByteId.class);

            if (reader.peek() != JsonToken.NAME) {
                return null;
            }
            if (!reader.nextName().equals("typeId")) {
                return null;
            }
            typeId = gson.fromJson(reader, ByteId.class);

            if(reader.peek() != JsonToken.NAME) {
                return null;
            }
            if(!reader.nextName().equals("fieldMap")) {
                return null;
            }
            if(reader.peek() != JsonToken.BEGIN_ARRAY) {
                return null;
            }
            reader.beginArray();
            
            TempTypeInfo typeInfo = new TempTypeInfo(typeName, dbId, typeId);
            
            while(reader.peek() != JsonToken.END_ARRAY) {
                TempFieldInfo fi = gson.fromJson(reader, TempFieldInfo.class);
                typeInfo.addField(fi);
            }
            
            reader.endArray();
            
            reader.endObject();
            return typeInfo;
        }
    }

}
