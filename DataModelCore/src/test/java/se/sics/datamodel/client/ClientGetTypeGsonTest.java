///*
// * This file is part of the CaracalDB distributed storage system.
// *
// * Copyright (C) 2009 Swedish Institute of Computer Science (SICS) 
// * Copyright (C) 2009 Royal Institute of Technology (KTH)
// *
// * This program is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// */
//
//package se.sics.datamodel.client;
//
//import com.google.gson.Gson;
//import junit.framework.Assert;
//import org.junit.Test;
//import se.sics.datamodel.gson.GsonHelper;
//import se.sics.datamodel.util.ByteId;
//
///**
// * @author Alex Ormenisan <aaor@sics.se>
// */
//public class ClientGetTypeGsonTest {
//    @Test
//    public void test() {
//        Gson gson = GsonHelper.getGson();
//        ByteId dbId = new ByteId(new byte[]{1, 1});
//        ByteId typeId = new ByteId(new byte[]{1, 2});
//        ClientGetTypeGson c = new ClientGetTypeGson(dbId, typeId);
//        System.out.println(c);
//        System.out.println(gson.toJson(c));
//        System.out.println(gson.fromJson(gson.toJson(c), ClientGetTypeGson.class));
//        ClientGetTypeGson cc = gson.fromJson("{\"dbId\":{\"id\":[1,1]},\"typeId\":{\"id\":[1,2]}}", ClientGetTypeGson.class);
//        System.out.println(cc);
//        Assert.assertEquals(c, cc);
//    }
//}
