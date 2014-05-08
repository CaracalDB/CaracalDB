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
package se.sics.datamodel.gson;

import se.sics.datamodel.gson.util.ValueHolder;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ValueHolderAdapter extends TypeAdapter<ValueHolder> {

    @Override
    public void write(JsonWriter writer, ValueHolder t) throws IOException {
        throw new UnsupportedOperationException("Should not write as ValueHolder, but as Value");
    }

    @Override
    public ValueHolder read(JsonReader reader) throws IOException {
        Gson gson = GsonHelper.getGson();
        ValueHolder.Builder vhBuilder = new ValueHolder.Builder();

        if (reader.peek() != JsonToken.BEGIN_OBJECT) {
            throw new IOException();
        }
        reader.beginObject();

        if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("fieldMap")) {
            throw new IOException();
        }
        if (reader.peek() != JsonToken.BEGIN_ARRAY) {
            throw new IOException();
        }
        reader.beginArray();

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
            JsonElement je = gson.fromJson(reader, JsonElement.class);

            vhBuilder.put(fieldId, je);

            if (reader.peek() != JsonToken.END_OBJECT) {
                throw new IOException();
            }
            reader.endObject();
        }

        reader.endArray();
        reader.endObject();
        return vhBuilder.build();
    }
}
