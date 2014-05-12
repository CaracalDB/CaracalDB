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
package se.sics.datamodel;

import se.sics.datamodel.TypeInfo.InconsistencyException;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public enum FieldType {

    STRING, INTEGER, LONG, BOOLEAN, FLOAT;

    public static FieldType parseString(String type) throws InconsistencyException {
        if (type != null) {
            if (type.toUpperCase().equals("STRING")) {
                return STRING;
            } else if (type.toUpperCase().equals("INTEGER") || type.toUpperCase().equals("INT")) {
                return INTEGER;
            } else if (type.toUpperCase().equals("LONG")) {
                return LONG;
            } else if (type.toUpperCase().equals("BOOLEAN")) {
                return BOOLEAN;
            } else if (type.toUpperCase().equals("FLOAT")) {
                return FLOAT;
            }
        }
        throw new InconsistencyException("Unknown type - expected type : " + FieldType.values());
    }
    
    public static Class<?> getFieldClass(FieldType ft) throws InconsistencyException {
        switch(ft) {
            case BOOLEAN: return Boolean.class;
            case STRING: return String.class;
            case INTEGER: return Integer.class;
            case LONG: return Long.class;
            case FLOAT: return Float.class;
            default: throw new InconsistencyException("Unknown type - expected type : " + FieldType.values());
        }
    }
}