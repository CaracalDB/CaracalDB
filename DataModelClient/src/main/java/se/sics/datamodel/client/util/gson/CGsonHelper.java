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
package se.sics.datamodel.client.util.gson;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import se.sics.datamodel.client.msg.CGetObj;
import se.sics.datamodel.client.msg.CGetType;
import se.sics.datamodel.client.msg.CPutObj;
import se.sics.datamodel.client.msg.CPutType;
import se.sics.datamodel.client.msg.CQueryObj;
import se.sics.datamodel.util.gson.GsonHelper;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class CGsonHelper {

    private static Gson gson = null;

    public static GsonBuilder getGsonBuilder() {
        GsonBuilder gsonBuilder = GsonHelper.getGsonBuilder();
        gsonBuilder.registerTypeAdapter(CGetType.class, new CGetTypeAdapter());
        gsonBuilder.registerTypeAdapter(CPutType.class, new CPutTypeAdapter());
        gsonBuilder.registerTypeAdapter(CGetObj.class, new CGetObjAdapter());
        gsonBuilder.registerTypeAdapter(CPutObj.class, new CPutObjAdapter());
        gsonBuilder.registerTypeAdapter(CQueryObj.class, new CQueryObjAdapter());
        return gsonBuilder;
    }
    
    public static Gson getGson() {
        if(gson == null) {
            gson = getGsonBuilder().create();
        }
        return gson;
    }
}
