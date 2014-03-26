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

import se.sics.datamodel.ValueHolder;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import se.sics.datamodel.ObjectValue;
import se.sics.datamodel.TypeInfo;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class GsonHelper {

    private static Gson gson = null;

    public static GsonBuilder getGsonBuilder() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(TypeInfo.class, new TypeInfoAdapter());
        gsonBuilder.registerTypeAdapter(ValueHolder.class, new ValueHolderAdapter());
        gsonBuilder.registerTypeAdapter(ObjectValue.class, new ObjectValueAdapter());
        return gsonBuilder;
    }

    public static Gson getGson() {
        if (gson == null) {
            gson = getGsonBuilder().create();
        }
        return gson;
    }
}
