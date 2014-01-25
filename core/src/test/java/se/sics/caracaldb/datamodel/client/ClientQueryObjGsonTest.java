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
package se.sics.caracaldb.datamodel.client;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import junit.framework.Assert;
import org.junit.Test;
import se.sics.caracaldb.datamodel.util.ByteId;
import se.sics.caracaldb.datamodel.util.GsonHelper;
import se.sics.caracaldb.datamodel.util.gsonextra.ClientQueryObjGson;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class ClientQueryObjGsonTest {

    @Test
    public void test() {
        Gson gson = GsonHelper.getGson();
        ByteId dbId = new ByteId(new byte[]{1, 1});
        ByteId typeId = new ByteId(new byte[]{1, 2});
        ByteId indexId = new ByteId(new byte[]{1, 3});
        JsonElement indexVal = gson.toJsonTree(15);

        ClientQueryObjGson c = new ClientQueryObjGson(dbId, typeId, indexId, indexVal);
        System.out.println(c);
        ClientQueryObjGson cc = gson.fromJson("{\"dbId\":{\"id\":[1,1]},\"typeId\":{\"id\":[1,2]},\"indexId\":{\"id\":[1,3]},\"indexValue\":15}", ClientQueryObjGson.class);
        System.out.println(cc);
        Assert.assertEquals(c, cc);
    }
}
