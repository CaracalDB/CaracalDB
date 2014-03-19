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
package se.sics.datamodel.util;

import com.google.gson.Gson;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
@RunWith(JUnit4.class)
public class TypeInfoTest {

    @Test
    public void typeInfoGsonTest() {
        Gson g = GsonHelper.getGson();

        ByteId dbId = new ByteId(new byte[]{1, 1});
        ByteId typeId = new ByteId(new byte[]{1, 2});
        TempTypeInfo typeInfo = new TempTypeInfo("type1", dbId, typeId);
      
        ByteIdFactory bif = new ByteIdFactory();
        typeInfo.addField("field1", FieldInfo.FieldType.FLOAT, true);
        typeInfo.addField("field2", FieldInfo.FieldType.STRING, false);
        typeInfo.addField("field3", FieldInfo.FieldType.FLOAT, true);
        
        System.out.println(typeInfo);
        System.out.println(g.toJson(typeInfo));
        System.out.println(g.fromJson(g.toJson(typeInfo), TempTypeInfo.class));
    }
}