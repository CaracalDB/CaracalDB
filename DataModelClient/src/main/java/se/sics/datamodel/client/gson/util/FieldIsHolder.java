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
package se.sics.datamodel.client.gson.util;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import se.sics.datamodel.FieldType;
import se.sics.datamodel.QueryType;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.client.gson.CGsonHelper;
import se.sics.datamodel.util.FieldIs;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class FieldIsHolder implements QueryTypeHolder {
    private final JsonElement indexVal;
    
    public FieldIsHolder(JsonElement indexVal) {
        this.indexVal = indexVal;
    }
    
    @Override
    public QueryType getQueryType(FieldType fieldType) {
        try {
            Gson gson = CGsonHelper.getGson();
            Object oindexVal = gson.fromJson(indexVal, FieldType.getFieldClass(fieldType));
            return new FieldIs(oindexVal);
        } catch (TypeInfo.InconsistencyException ex) {
            throw new RuntimeException(ex);
        }
    }    
}