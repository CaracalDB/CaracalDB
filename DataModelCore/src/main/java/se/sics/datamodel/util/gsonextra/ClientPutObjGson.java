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
import se.sics.datamodel.util.TempObject.ValueHolder;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ClientPutObjGson {
    public final ByteId dbId;
    public final ByteId typeId;
    public final ByteId objId;
    public final ValueHolder objValue;

    public ClientPutObjGson(ByteId dbId, ByteId typeId, ByteId objId, ValueHolder objValue) {
        this.dbId = dbId;
        this.typeId = typeId;
        this.objId = objId;
        this.objValue = objValue;
    }
    
    public static class GsonTypeAdapter extends TypeAdapter<ClientPutObjGson> {

        @Override
        public void write(JsonWriter writer, ClientPutObjGson t) throws IOException {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public ClientPutObjGson read(JsonReader reader) throws IOException {
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
            
            if(reader.peek() != JsonToken.NAME || !reader.nextName().equals("objValue")) {
                return null;
            }
            ValueHolder objValue = gson.fromJson(reader, ValueHolder.class);
            
            reader.endObject();
            return new ClientPutObjGson(dbId, typeId, objId, objValue);
        }
    }
}