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
package se.sics.caracaldb.ycsb;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.client.BlockingClient;
import se.sics.caracaldb.client.ClientManager;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.ResponseCode;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class CaracalDB extends DB {

    private BlockingClient client;

    @Override
    public void init() throws DBException {
        client = ClientManager.newClient();
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        Key k = new Key(key);
        GetResponse resp = client.get(k);
        if (resp.code != ResponseCode.SUCCESS) {
            return 2;
        }
        if (resp.data == null) {
            return 1;
        }
        result.put(key, new ByteArrayByteIterator(resp.data));
        return 0;
    }

    @Override
    public int scan(String string, String string1, int i, Set<String> set, Vector<HashMap<String, ByteIterator>> vector) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        return put(key, values);
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        return put(key, values);
    }

    @Override
    public int delete(String table, String key) {
        return put(key, null);
    }

    private int put(String key, HashMap<String, ByteIterator> values) {
        Key k = new Key(key);
        byte[] val = null;
        if (values != null) {
            val = values.values().iterator().next().toArray();
        }
        ResponseCode resp = client.put(k, val);
        if (resp != ResponseCode.SUCCESS) {
            return 2;
        }
        return 0;
    }
}
