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

import org.javatuples.Pair;
import org.junit.Assert;
import org.junit.Test;
import se.sics.datamodel.FieldType;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.DMSerializer;
import se.sics.datamodel.FieldInfo;
import se.sics.datamodel.ObjectValue;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.gson.util.ValueHolder;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class SerializerTest {

//    @Test
//    public void testByteId() {
//        byte[] serialized;
//        ByteId original, deserialized;
//        String soriginal;
//        
//        original = new ByteId(new byte[]{1, 5});
//        serialized = DMSerializer.serialize(original);
//        deserialized = DMSerializer.deserialize(serialized, ByteId.class);
//        Assert.assertEquals(original, deserialized);
//        soriginal = "[1,5]";
//        deserialized = DMSerializer.fromString(soriginal, ByteId.class);
//        Assert.assertEquals(original, deserialized);
//        
//        original = new ByteId(new byte[]{4, 1 , 1, 3, 11});
//        serialized = DMSerializer.serialize(original);
//        deserialized = DMSerializer.deserialize(serialized, ByteId.class);
//        Assert.assertEquals(original, deserialized);
//        soriginal = "[4,1,1,3,11]";
//        deserialized = DMSerializer.fromString(soriginal, ByteId.class);
//        Assert.assertEquals(original, deserialized);
//    }
//    @Test
//    public void testFieldType() {
//        FieldType[] fTypes = FieldType.values();
//
//        for (FieldType ft : fTypes) {
//            byte[] b = DMSerializer.serialize(ft);
//            FieldType deserializedFT = DMSerializer.deserialize(b, FieldType.class);
//            Assert.assertEquals(ft, deserializedFT);
//        }
//    }
//    @Test
//    public void testFieldInfo() {
//        FieldInfo original, deserialized;
//        String soriginal;
//        byte[] serialized;
//        
//        original = new FieldInfo("field1", FieldType.STRING, true);
//        serialized = DMSerializer.serialize(original);
//        deserialized = DMSerializer.deserialize(serialized, FieldInfo.class);
//        Assert.assertEquals(original, deserialized);
//        soriginal = "\"name\":\"field1\",\"type\":STRING,\"indexed\":true";
//        deserialized = DMSerializer.fromString(soriginal, FieldInfo.class);
//        Assert.assertEquals(original, deserialized);
//    }
    @Test
    public void testTypeInfo() {
        TypeInfo original, deserialized;
        String soriginal;
        byte[] serialized;

        TypeInfo.Builder tiBuilder = new TypeInfo.Builder("type1");
        tiBuilder.put(new ByteId(new byte[]{1, 1}), new FieldInfo("field1", FieldType.INTEGER, false));
        tiBuilder.put(new ByteId(new byte[]{1, 2}), new FieldInfo("field2", FieldType.STRING, false));
        tiBuilder.put(new ByteId(new byte[]{1, 3}), new FieldInfo("field3", FieldType.BOOLEAN, false));
        original = tiBuilder.build();

        serialized = DMSerializer.serialize(original);
        System.out.println(DMSerializer.asString(original));
        deserialized = DMSerializer.deserialize(serialized, TypeInfo.class);
        Assert.assertEquals(original, deserialized);
        soriginal = "{\"typeName\":\"type1\",\"fieldMap\":[";
        soriginal = soriginal + "{\"id\":[1,1],\"field\":{\"name\":\"field1\",\"type\":\"INTEGER\",\"indexed\":false}}";
        soriginal = soriginal + ",{\"id\":[1,2],\"field\":{\"name\":\"field2\",\"type\":\"STRING\",\"indexed\":false}}";
        soriginal = soriginal + ",{\"id\":[1,3],\"field\":{\"name\":\"field3\",\"type\":\"BOOLEAN\",\"indexed\":false}}";
        soriginal = soriginal + "]}";
        System.out.println(soriginal);
        deserialized = DMSerializer.fromString(soriginal, TypeInfo.class);
        Assert.assertEquals(original, deserialized);
    }

    
    @Test
    public void testObject() throws TypeInfo.InconsistencyException {
        TypeInfo ti;
        ObjectValue original, deserialized;
        String soriginal;

        TypeInfo.Builder tiBuilder = new TypeInfo.Builder("type1");
        tiBuilder.put(new ByteId(new byte[]{1,1}), new FieldInfo("field1",FieldType.INTEGER, false));
        tiBuilder.put(new ByteId(new byte[]{1,2}), new FieldInfo("field2",FieldType.STRING, false));
        tiBuilder.put(new ByteId(new byte[]{1,3}), new FieldInfo("field3",FieldType.BOOLEAN, false));
        ti = tiBuilder.build();
        
        ObjectValue.Builder ovBuilder = new ObjectValue.Builder();
        ovBuilder.put(new ByteId(new byte[]{1, 1}), new Integer(11));
        ovBuilder.put(new ByteId(new byte[]{1, 2}), "some");
        ovBuilder.put(new ByteId(new byte[]{1, 3}), Boolean.TRUE);
        original = ovBuilder.build();
        
        soriginal = "{\"fieldMap\":[{\"id\":[1,1],\"field\":11},{\"id\":[1,2],\"field\":\"some\"},{\"id\":[1,3],\"field\":true}]}";
        Assert.assertEquals(soriginal, DMSerializer.asString(original));
        deserialized = DMSerializer.fromString(soriginal, ValueHolder.class).getObjectValue(ti);
        Assert.assertEquals(original, deserialized);
    }
}
