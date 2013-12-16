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

package se.sics.caracaldb.datamodel.util;

import com.google.gson.Gson;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class TempTypeInfo {
    public final ByteId dbId;
    public final ByteId typeId;
    public final Map<ByteId, TempFieldInfo> fieldMap;
    
    public TempTypeInfo(ByteId dbId, ByteId typeId) {
        this.dbId = dbId;
        this.typeId = typeId;
        this.fieldMap = new TreeMap<ByteId, TempFieldInfo>();
    } 
    
    public void deserializeField(byte[] bFieldInfo) throws UnsupportedEncodingException {
        deserializeField(new String(bFieldInfo, "UTF8"));
    }
    
    public void deserializeField(String sFieldInfo) {
        Gson gson = new Gson();
        TempFieldInfo fieldInfo = gson.fromJson(sFieldInfo, TempFieldInfo.class);
        fieldMap.put(new ByteId(fieldInfo.fieldId), fieldInfo);
    }
    
    public static byte[] serializeField(TempFieldInfo fieldInfo) throws UnsupportedEncodingException {
        Gson gson = new Gson();
        String s = gson.toJson(fieldInfo);
        return s.getBytes("UTF8");
    }
    
    public static class TempFieldInfo {
        private byte[] fieldId;
        private String fieldName;
        private FieldInfo.FieldType fieldType;
        private boolean indexed;
        
        TempFieldInfo() {
        }
        
        TempFieldInfo(byte[] fieldId, String fieldName, FieldInfo.FieldType fieldType, boolean indexed) {
            this.fieldId = fieldId;
            this.fieldName = fieldName;
            this.fieldType = fieldType;
            this.indexed = indexed;
        }
    }
}