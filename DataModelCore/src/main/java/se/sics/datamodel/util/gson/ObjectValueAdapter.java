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
import com.google.gson.stream.JsonWriter;
import java.io.IOException;
import java.util.Map;
import se.sics.datamodel.ObjectValue;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class ObjectValueAdapter extends TypeAdapter<ObjectValue>{

    @Override
    public void write(JsonWriter writer, ObjectValue t) throws IOException {
        Gson gson = GsonHelper.getGson();

        writer.beginObject();
        writer.name("fieldMap");
        writer.beginArray();
        for (Map.Entry<ByteId, Object> e : t.entrySet()) {
            writer.beginObject();
            gson.toJson(gson.toJsonTree(e.getKey().getId()), writer.name("id"));
            gson.toJson(gson.toJsonTree(e.getValue()), writer.name("field"));
            writer.endObject();
        }
        writer.endArray();
        writer.endObject();
    }

    @Override
    public ObjectValue read(JsonReader reader) throws IOException {
        throw new UnsupportedOperationException("Should not write as ValueHolder, but as Value");
    }
}
