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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class TypeInfo {

    public final ByteId dbId;
    public final ByteId typeId;
    private final ByteIdFactory bidFactory;
    private final Map<ByteId, FieldInfo> fieldMap;   // <fieldId, fieldInfo>
    private final Set<ByteId> indexSet; 	     // <indexId>

    public TypeInfo(ByteId dbId, ByteId typeId) {
        this.dbId = dbId;
        this.typeId = typeId;
        this.bidFactory = new ByteIdFactory();
        this.fieldMap = new TreeMap<ByteId, FieldInfo>();
        this.indexSet = new TreeSet<ByteId>();
    }

    public void addField(ByteId fieldId, String fieldName, String sType) throws TypeInfo.InconsistencyException {
        FieldInfo.FieldType fieldType = FieldInfo.FieldType.parseString(sType);
        fieldMap.put(fieldId, new FieldInfo(fieldId, fieldName.toLowerCase(), fieldType));
    }

    public void addField(String fieldName, String sType) throws TypeInfo.InconsistencyException {
        ByteId nextId = bidFactory.nextId();
        FieldInfo.FieldType fieldType = FieldInfo.FieldType.parseString(sType);
        fieldMap.put(nextId, new FieldInfo(nextId, fieldName.toLowerCase(), fieldType));
    }
    
//    public void deserializeField(ByteId fieldId, String value) {
//        Gson gson = new Gson();
//        TempFieldInfo tfInfo = gson.fromJson(value, TempFieldInfo.class);
//        FieldInfo fInfo = new FieldInfo(new ByteId(tfInfo.fieldId), tfInfo.fieldName, tfInfo.fieldType);
//        fieldMap.put(fInfo.fieldId, fInfo);
//        if(tfInfo.indexed) {
//            indexSet.add(fInfo.fieldId);
//        }
//    }
//    
//    public String serializeField(ByteId fieldId) {
//        FieldInfo fInfo = fieldMap.get(fieldId);
//        boolean indexed = indexSet.contains(fieldId);
//        TempFieldInfo tfInfo = new TempFieldInfo(fInfo.fieldId.getId(), fInfo.fieldName, fInfo.fieldType, indexed);
//        Gson gson = new Gson();
//        return gson.toJson(null)
//    }

    public ByteId getFieldId(String fieldName) {
        for (FieldInfo field : fieldMap.values()) {
            if (field.fieldName.equals(fieldName)) {
                return field.fieldId;
            }
        }
        return null;
    }

    public static class InconsistencyException extends Exception {

        public InconsistencyException(String message) {
            super(message);
        }
    }
    
}
