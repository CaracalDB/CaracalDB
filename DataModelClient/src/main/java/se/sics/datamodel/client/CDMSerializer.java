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

package se.sics.datamodel.client;

import java.io.UnsupportedEncodingException;
import se.sics.datamodel.client.util.gson.CGsonHelper;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */


public class CDMSerializer {
    public static <T> String asString(T o) {
        return CGsonHelper.getGson().toJson(o);
    }
    
    public static <T> T fromString(String s, Class<T> type) {
        return CGsonHelper.getGson().fromJson(s, type);
    }
    
    public static <T> byte[] serialize(T o) {
        try {
            return CGsonHelper.getGson().toJson(o).getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static <T> T deserialize(byte[] b, Class<T> type) {
        String s;
        try {
            s = new String(b, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        return CGsonHelper.getGson().fromJson(s, type);
    }
}