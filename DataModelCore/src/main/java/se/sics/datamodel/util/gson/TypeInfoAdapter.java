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
package se.sics.datamodel.util.gson;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Map;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.FieldInfo;
import se.sics.datamodel.FieldType;
import se.sics.datamodel.TypeInfo;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class TypeInfoAdapter extends TypeAdapter<TypeInfo> {

    @Override
    public void write(JsonWriter writer, TypeInfo ti) throws IOException {
        Gson gson = GsonHelper.getGson();

        writer.beginObject();
        writer.name("typeName").value(ti.name);
        writer.name("fieldMap").beginArray();
        for (Map.Entry<ByteId, FieldInfo> e : ti.entrySet()) {
            writer.beginObject();
            gson.toJson(gson.toJsonTree(e.getKey().getId()), writer.name("id"));
            gson.toJson(gson.toJsonTree(e.getValue()),writer.name("field"));
            writer.endObject();
        }
        writer.endArray();
        writer.endObject();
    }

    @Override
    public TypeInfo read(JsonReader reader) throws IOException {
        String typeName;

        Gson gson = GsonHelper.getGson();

        reader.beginObject();
        if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("typeName")) {
            throw new IOException();
        }
        typeName = reader.nextString();

        if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("fieldMap")) {
            throw new IOException();
        }
        if (reader.peek() != JsonToken.BEGIN_ARRAY) {
            throw new IOException();
        }
        reader.beginArray();

        TypeInfo.Builder tiBuilder = new TypeInfo.Builder(typeName);

        while (reader.peek() != JsonToken.END_ARRAY) {
            if (reader.peek() != JsonToken.BEGIN_OBJECT) {
                throw new IOException();
            }
            reader.beginObject();

            if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("id")) {
                throw new IOException();
            }
            ByteId fieldId = new ByteId((byte[])gson.fromJson(reader, byte[].class));
            
            if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("field")) {
                throw new IOException();
            }
            FieldInfo fieldInfo = gson.fromJson(reader, FieldInfo.class);

            tiBuilder.put(fieldId, fieldInfo);
            reader.endObject();
        }

        reader.endArray();
        reader.endObject();
        return tiBuilder.build();
    }
    
    private void writeFieldInfo(JsonWriter writer, FieldInfo fi) throws IOException {
        Gson gson = GsonHelper.getGson();
        writer.name("name").value(fi.name);
        gson.toJson(gson.toJsonTree(fi.type), writer.name("type"));
        writer.name("indexed").value(fi.indexed);
    }
    
    private FieldInfo readFieldInfo(JsonReader reader) throws IOException {
        String fieldName;
        FieldType fieldType;
        Boolean indexed;
        
        Gson gson = GsonHelper.getGson();
        
        if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("name")) {
            throw new IOException();
        }
        fieldName = gson.fromJson(reader, String.class);
        
        if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("type")) {
            throw new IOException();
        }
        fieldType = gson.fromJson(reader, FieldType.class);
        
        if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("indexed")) {
            throw new IOException();
        }
        indexed = gson.fromJson(reader, Boolean.class);
        
        return new FieldInfo(fieldName, fieldType, indexed);
    }
}
