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

package se.sics.datamodel.util.gsonextra;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.gson.GsonHelper;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ClientQueryObjGson {
    public final ByteId dbId;
    public final ByteId typeId;
    public final ByteId indexId;
    public final JsonElement indexValue;

    public ClientQueryObjGson(ByteId dbId, ByteId typeId, ByteId indexId, JsonElement indexValue) {
        this.dbId = dbId;
        this.typeId = typeId;
        this.indexId = indexId;
        this.indexValue = indexValue;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("dbId: ").append(dbId.toString()).append("\n");
        sb.append("typeId: ").append(typeId.toString()).append("\n");
        sb.append("indexId: ").append(indexId.toString()).append("\n");
        sb.append("indexValue: ").append(indexValue.toString()).append("\n");
        return sb.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        if(!(obj instanceof ClientQueryObjGson)) {
            return false;
        }
        ClientQueryObjGson typedObj = (ClientQueryObjGson)obj;
        if(!dbId.equals(typedObj.dbId)) {
            return false;
        }
        if(!typeId.equals(typedObj.typeId)) {
            return false;
        }
        if(!indexId.equals(typedObj.indexId)) {
            return false;
        }
        if(!indexValue.equals(typedObj.indexValue)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 97 * hash + (this.dbId != null ? this.dbId.hashCode() : 0);
        hash = 97 * hash + (this.typeId != null ? this.typeId.hashCode() : 0);
        hash = 97 * hash + (this.indexId != null ? this.indexId.hashCode() : 0);
        hash = 97 * hash + (this.indexValue != null ? this.indexValue.hashCode() : 0);
        return hash;
    }
    
    public static class GsonTypeAdapter extends TypeAdapter<ClientQueryObjGson> {

        @Override
        public void write(JsonWriter writer, ClientQueryObjGson t) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); 
        }

        @Override
        public ClientQueryObjGson read(JsonReader reader) throws IOException {
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
            if(reader.peek() != JsonToken.NAME || !reader.nextName().equals("dbId")) {
                return null;
            }
            ByteId dbId = gson.fromJson(reader, ByteId.class);
            
            if(reader.peek() != JsonToken.NAME || !reader.nextName().equals("typeId")) {
                return null;
            }
            ByteId typeId = gson.fromJson(reader, ByteId.class);
            
            if(reader.peek() != JsonToken.NAME || !reader.nextName().equals("indexId")) {
                return null;
            }
            ByteId indexId = gson.fromJson(reader, ByteId.class);
            
            if(reader.peek() != JsonToken.NAME || !reader.nextName().equals("indexValue")) {
                return null;
            }
            JsonElement indexVal = gson.fromJson(reader, JsonElement.class);
            
            reader.endObject();
            return new ClientQueryObjGson(dbId, typeId, indexId, indexVal);
        }
    }
}