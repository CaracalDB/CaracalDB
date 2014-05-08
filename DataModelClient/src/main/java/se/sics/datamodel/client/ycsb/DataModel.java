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

import com.google.common.primitives.Longs;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.caracaldb.store.Limit;
import se.sics.datamodel.FieldInfo;
import se.sics.datamodel.FieldType;
import se.sics.datamodel.ObjectValue;
import se.sics.datamodel.QueryType;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.client.BlockingClient;
import se.sics.datamodel.client.ClientManager;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.util.FieldScan;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class DataModel extends DB {
    private BlockingClient client;
    private Pair<ByteId,ByteId> typeId;
    private TypeInfo typeInfo;

    @Override
    public void init() throws DBException {
        client = ClientManager.newClient();
        //hardcoded for the moment
        this.typeId = Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,1}));
        this.typeInfo = getType();
        client.putType(typeId, typeInfo);
    }
    
    private TypeInfo getType() {
        TypeInfo.Builder tiBuilder = new TypeInfo.Builder("type1");
        tiBuilder.put(new ByteId(new byte[]{1,1}), new FieldInfo("field0", FieldType.STRING, true));
        tiBuilder.put(new ByteId(new byte[]{1,2}), new FieldInfo("field1", FieldType.STRING, false));
        return tiBuilder.build();
    }
    
    private Triplet<ByteId,ByteId,ByteId> getObjId(String key) {
        Long lkey = Long.parseLong(key);
        byte[] byteKey = Longs.toByteArray(lkey);
        if(byteKey.length > 8) {
            throw new RuntimeException("" + byteKey.length);
        }
        byte[] bkey = new byte[9];
        bkey[0] = (byte)8;
        System.arraycopy(byteKey, 0, bkey, 9-byteKey.length, byteKey.length);
        return typeId.add(new ByteId(bkey));
    }
    
    private ObjectValue getObj(HashMap<String, ByteIterator> val) {
        ObjectValue.Builder ovBuilder = new ObjectValue.Builder();
        if(val.size() != typeInfo.size()) {
            throw new RuntimeException("Wrong object");
        }
        for(Entry<ByteId, FieldInfo> e : typeInfo.entrySet()) {
            byte[] bfieldVal = val.get(e.getValue().name).toArray();
//            Class fieldType;
//            try {
//                fieldType = FieldType.getFieldClass(e.getValue().type);
//            } catch (TypeInfo.InconsistencyException ex) {
//                throw new RuntimeException(ex);
//            }
            String fieldVal = new String(bfieldVal);
            ovBuilder.put(e.getKey(), fieldVal);
        }
        return ovBuilder.build();
    } 

    @Override
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        Triplet<ByteId,ByteId,ByteId> objId = getObjId(key);
        Pair<DMMessage.ResponseCode, byte[]> resp = client.getObj(objId);
        if(resp.getValue0() == DMMessage.ResponseCode.SUCCESS) {
            return 0;
        }
        System.out.println("fail");
        return 0;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        Triplet<ByteId,ByteId,ByteId> objId = getObjId(key);
        ObjectValue objVal = getObj(values);
        DMMessage.ResponseCode resp = client.putObj(objId, objVal, typeInfo);
        if(resp == DMMessage.ResponseCode.SUCCESS) {
            return 0;
        }
        System.out.println("fail");
        return 0;
    }

    @Override
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        Triplet<ByteId,ByteId,ByteId> objId = getObjId(key);
        ObjectValue objVal = getObj(values);
        DMMessage.ResponseCode resp = client.putObj(objId, objVal, typeInfo);
        if(resp == DMMessage.ResponseCode.SUCCESS) {
            return 0;
        }
        System.out.println("fail");
        return 0;
    }

    @Override
    public int delete(String table, String key) {
        System.out.println("delete");
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        Iterator<String> it = fields.iterator();
        String from = it.next();
        String to = it.next();
        if(from.compareTo(to) > 0) {
            String aux = from;
            from = to;
            to = from;
        }
        QueryType qt = new FieldScan(from, to);
        Pair<DMMessage.ResponseCode, Map<ByteId, byte[]>> resp = client.queryObj(typeId, new ByteId(new byte[]{1,1}), qt, Limit.toItems(recordcount));
        if(resp.getValue1() == null || resp.getValue1().size() == 0) {
            System.out.println(0);
        }
        if(resp.getValue0() == DMMessage.ResponseCode.SUCCESS) {
            return 0;
        }
        System.out.println("fail");
        return 0;
    }
}