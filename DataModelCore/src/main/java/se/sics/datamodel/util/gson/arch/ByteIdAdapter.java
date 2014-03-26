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
//package se.sics.datamodel.util.gson.arch;
//
//import com.google.gson.Gson;
//import com.google.gson.TypeAdapter;
//import com.google.gson.stream.JsonReader;
//import com.google.gson.stream.JsonWriter;
//import java.io.IOException;
//import se.sics.datamodel.util.ByteId;
//import se.sics.datamodel.util.gson.GsonHelper;
//
///**
// * @author Alex Ormenisan <aaor@sics.se>
// */
//
//
//public class ByteIdAdapter extends TypeAdapter<ByteId> {
//
//    @Override
//    public void write(JsonWriter writer, ByteId bid) throws IOException {
//         Gson gson = GsonHelper.getGson();
//         gson.toJson(gson.toJsonTree(bid.getId()), writer);
//    }
//
//    @Override
//    public ByteId read(JsonReader reader) throws IOException {
//        Gson gson = GsonHelper.getGson();
//        byte[] bid = gson.fromJson(reader, byte[].class);
//        return new ByteId(bid);
//    }
//}