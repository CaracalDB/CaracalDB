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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 *
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class TypeInfo {

    public final int dbId;
    public final String typeName;
    private int typeId;

    private Map<Integer, FieldInfo> fieldMap;   // <fieldId, fieldInfo>
    private Set<Integer> indexSet; 		     // <indexId>
    private int nextFieldId = 0;

    public TypeInfo(int dbId, String typeName) {
        this.dbId = dbId;
        this.typeId = -1;
        this.typeName = typeName;

        this.fieldMap = new TreeMap<Integer, FieldInfo>();
        this.indexSet = new TreeSet<Integer>();
    }

    public TypeInfo(int dbId, int typeId, String typeName) {
        this.dbId = dbId;
        this.typeId = typeId;
        this.typeName = typeName;
        this.fieldMap = new TreeMap<Integer, FieldInfo>();
        this.indexSet = new TreeSet<Integer>();
    }

    public void setTypeId(int typeId) {
        this.typeId = typeId;
    }

    public int getTypeId() {
        return typeId;
    }

    public void addField(int fieldId, String fieldName, String sType) throws TypeInfo.InconsistencyException {
        FieldInfo.FieldType fieldType = FieldInfo.FieldType.parseString(sType);
        fieldMap.put(fieldId, new FieldInfo(fieldId, fieldName.toLowerCase(), fieldType));
    }

    public void addField(String fieldName, String sType) throws TypeInfo.InconsistencyException {
        nextFieldId++;
        FieldInfo.FieldType fieldType = FieldInfo.FieldType.parseString(sType);
        fieldMap.put(nextFieldId, new FieldInfo(nextFieldId, fieldName.toLowerCase(), fieldType));
    }

    public int getFieldId(String fieldName) {
        for (FieldInfo field : fieldMap.values()) {
            if (field.fieldName.equals(fieldName)) {
                return field.fieldId;
            }
        }
        return -1;
    }

    public static class InconsistencyException extends Exception {

        public InconsistencyException(String message) {
            super(message);
        }
    }
}
