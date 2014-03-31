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
package se.sics.datamodel.client.ycsb;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import se.sics.datamodel.client.BlockingClient;
import se.sics.datamodel.client.ClientManager;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class DataModelTest extends DB {
    private BlockingClient client;

    @Override
    public void init() throws DBException {
        client = ClientManager.newClient();
    }

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        System.out.println(table + " " + key + " " + fields);
        return 0;
    }

    @Override
    public int scan(String lower, String upper, int limit, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        System.out.println(lower + " " + upper + " " + limit + " " + fields);
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        System.out.println(table + " " + key + " " + values);
        return 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        System.out.println(table + " " + key + " " + values);
        return 0;
    }

    @Override
    public int delete(String table, String key) {
        System.out.println(table + " " + key);
        return 0;
    }
}