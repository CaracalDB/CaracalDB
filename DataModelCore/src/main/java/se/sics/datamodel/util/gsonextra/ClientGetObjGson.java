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
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.util.GsonHelper;
import se.sics.datamodel.util.TempObject;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ClientGetObjGson {
    public final ByteId dbId;
    public final ByteId typeId;
    public final ByteId objId;

    public ClientGetObjGson(ByteId dbId, ByteId typeId, ByteId objId) {
        this.dbId = dbId;
        this.typeId = typeId;
        this.objId = objId;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("dbId: ").append(dbId.toString()).append("\n");
        sb.append("typeId: ").append(typeId.toString()).append("\n");
        sb.append("objId: ").append(objId.toString()).append("\n");
        return sb.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }
        if(!(obj instanceof ClientGetObjGson)) {
            return false;
        }
        ClientGetObjGson typedObj = (ClientGetObjGson)obj;
        if(!dbId.equals(typedObj.dbId)) {
            return false;
        }
        if(!typeId.equals(typedObj.typeId)) {
            return false;
        }
        if(!objId.equals(typedObj.objId)) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + (this.dbId != null ? this.dbId.hashCode() : 0);
        hash = 97 * hash + (this.typeId != null ? this.typeId.hashCode() : 0);
        hash = 97 * hash + (this.objId != null ? this.objId.hashCode() : 0);
        return hash;
    }
    
    public static class GsonTypeAdapter extends TypeAdapter<ClientGetObjGson> {

        @Override
        public void write(JsonWriter writer, ClientGetObjGson t) throws IOException {
            throw new UnsupportedOperationException("Not supported yet."); 
        }

        @Override
        public ClientGetObjGson read(JsonReader reader) throws IOException {
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
            
            if(reader.peek() != JsonToken.NAME || !reader.nextName().equals("objId")) {
                return null;
            }
            ByteId objId = gson.fromJson(reader, ByteId.class);
            
            reader.endObject();
            return new ClientGetObjGson(dbId, typeId, objId);
        }
        
    }
}
