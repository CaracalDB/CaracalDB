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
package se.sics.datamodel.client.gson;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import org.javatuples.Pair;
import se.sics.datamodel.client.gson.msg.CGetType;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class CGetTypeAdapter extends TypeAdapter<CGetType> {

    @Override
    public void write(JsonWriter writer, CGetType m) throws IOException {
        throw new UnsupportedOperationException("Only read supported");
    }

    @Override
    public CGetType read(JsonReader reader) throws IOException {
        Gson gson = CGsonHelper.getGson();

        reader.beginObject();
        if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("dbId")) {
            throw new IOException();
        }
        ByteId dbId = new ByteId((byte[]) gson.fromJson(reader, byte[].class));

        if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("typeId")) {
            throw new IOException();
        }
        ByteId typeId = new ByteId((byte[]) gson.fromJson(reader, byte[].class));

        reader.endObject();
        return new CGetType(Pair.with(dbId, typeId));
    }
}