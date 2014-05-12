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
import com.google.gson.JsonElement;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import se.sics.datamodel.client.gson.util.FieldIsHolder;
import se.sics.datamodel.client.gson.util.FieldScanHolder;
import se.sics.datamodel.client.gson.util.QueryTypeHolder;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class CQueryTypeAdapter extends TypeAdapter<QueryTypeHolder> {

    @Override
    public void write(JsonWriter writer, QueryTypeHolder t) throws IOException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public QueryTypeHolder read(JsonReader reader) throws IOException {
        Gson gson = CGsonHelper.getGson();

        reader.beginObject();
        if (reader.peek() != JsonToken.NAME) {
            throw new IOException();
        }
        String queryType = reader.nextName();
        if (queryType.equals("is")) {
            JsonElement je = gson.fromJson(reader, JsonElement.class);
            return new FieldIsHolder(je);
        } else if (queryType.equals("from")) {
            JsonElement jeFrom = gson.fromJson(reader, JsonElement.class);
            if (reader.peek() != JsonToken.NAME || !reader.nextName().equals("to")) {
                throw new IOException();
            }
            JsonElement jeTo = gson.fromJson(reader, JsonElement.class);
            return new FieldScanHolder(jeFrom, jeTo);
        } else {
            throw new IOException("unknown query type");
        }
    }
}