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
package se.sics.datamodel.simulation.cmd;

import com.google.gson.Gson;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.caracaldb.store.Limit;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.GetObj;
import se.sics.datamodel.msg.GetType;
import se.sics.datamodel.msg.PutObj;
import se.sics.datamodel.msg.PutType;
import se.sics.datamodel.msg.QueryObj;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.util.ByteIdFactory;
import se.sics.datamodel.gson.GsonHelper;
import se.sics.datamodel.simulation.DMExperiment;
import se.sics.datamodel.simulation.validators.GetObjValidator;
import se.sics.datamodel.simulation.validators.GetTypeValidator;
import se.sics.datamodel.simulation.validators.PutObjValidator;
import se.sics.datamodel.simulation.validators.PutTypeValidator;
import se.sics.datamodel.simulation.validators.QueryObjValidator;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.datamodel.DMSerializer;
import se.sics.datamodel.FieldInfo;
import se.sics.datamodel.FieldType;
import se.sics.datamodel.IndexHelper;
import se.sics.datamodel.ObjectValue;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.util.FieldIs;
import se.sics.datamodel.util.FieldScan;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

public class DMExp1Cmd extends DMExpCmd {
    @Override
    public DMExperiment getExp() {
        try {
            Gson gson = GsonHelper.getGson();
            
            DMExperiment.Builder builder = new DMExperiment.Builder();
            TimestampIdFactory tidFactory = TimestampIdFactory.get();
            ByteIdFactory bidFactory = new ByteIdFactory();
            
            Pair<ByteId,ByteId> nonExistentType = Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,1}));
            Pair<ByteId,ByteId> typeId1 = Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2}));
            Pair<ByteId,ByteId> typeId2 = Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,3}));
            
            TypeInfo typeInfo1 = createType1();
            TypeInfo typeInfo2 = createType2();
            
            Triplet<ByteId, ByteId, ByteId> objId1 = typeId1.add(new ByteId(new byte[]{1,2}));
            Triplet<ByteId, ByteId, ByteId> objId2 = typeId2.add(new ByteId(new byte[]{1,1}));
            Triplet<ByteId, ByteId, ByteId> objId3 = typeId2.add(new ByteId(new byte[]{1,2}));
            Triplet<ByteId, ByteId, ByteId> objId4 = typeId2.add(new ByteId(new byte[]{1,3}));
            
            ObjectValue obj1_type1 = createObj1_Type1();
            ObjectValue obj2_type2 = createObj2_Type2();
            ObjectValue obj3_type2 = createObj3_Type2();
            ObjectValue obj4_type2 = createObj4_Type2();
            
            GetType.Req getType_req1 = new GetType.Req(tidFactory.newId(), nonExistentType);
            GetTypeValidator getType_val1 = new GetTypeValidator(getType_req1.id, DMMessage.ResponseCode.SUCCESS, null);
            builder.add(getType_req1, getType_val1);
            
            PutType.Req putType_req2 = new PutType.Req(tidFactory.newId(), typeId1, DMSerializer.serialize(typeInfo1));
            PutTypeValidator putType_val2 = new PutTypeValidator(putType_req2.id, DMMessage.ResponseCode.SUCCESS);
            builder.add(putType_req2, putType_val2);
            
            GetType.Req getType_req3 = new GetType.Req(tidFactory.newId(), typeId1);
            GetTypeValidator getType_val3 = new GetTypeValidator(getType_req3.id, DMMessage.ResponseCode.SUCCESS, DMSerializer.serialize(typeInfo1));
            builder.add(getType_req3, getType_val3);
            
            PutObj.Req putObj_req4 = new PutObj.Req(tidFactory.newId(), objId1, DMSerializer.serialize(obj1_type1), IndexHelper.getIndexes(typeInfo1, obj1_type1));
            PutObjValidator putObj_val4 = new PutObjValidator(putObj_req4.id, DMMessage.ResponseCode.SUCCESS);
            builder.add(putObj_req4, putObj_val4);
            
            GetObj.Req getObj_req5 = new GetObj.Req(tidFactory.newId(), objId1);
            GetObjValidator getObj_val5 = new GetObjValidator(getObj_req5.id, DMMessage.ResponseCode.SUCCESS, DMSerializer.serialize(obj1_type1));
            builder.add(getObj_req5, getObj_val5);
            
            PutType.Req putType_req6 = new PutType.Req(tidFactory.newId(), typeId2, DMSerializer.serialize(typeInfo2));
            PutTypeValidator putType_val6 = new PutTypeValidator(putType_req6.id, DMMessage.ResponseCode.SUCCESS);
            builder.add(putType_req6, putType_val6);
            
            PutObj.Req putObj_req7 = new PutObj.Req(tidFactory.newId(), objId2, DMSerializer.serialize(obj2_type2), IndexHelper.getIndexes(typeInfo2, obj2_type2));
            PutObjValidator putObj_val7 = new PutObjValidator(putObj_req7.id, DMMessage.ResponseCode.SUCCESS);
            builder.add(putObj_req7, putObj_val7);
            
            PutObj.Req putObj_req8 = new PutObj.Req(tidFactory.newId(), objId3, DMSerializer.serialize(obj3_type2), IndexHelper.getIndexes(typeInfo2, obj3_type2));
            PutObjValidator putObj_val8 = new PutObjValidator(putObj_req8.id, DMMessage.ResponseCode.SUCCESS);
            builder.add(putObj_req8, putObj_val8);
            
            PutObj.Req putObj_req9 = new PutObj.Req(tidFactory.newId(), objId4, DMSerializer.serialize(obj4_type2), IndexHelper.getIndexes(typeInfo2, obj4_type2));
            PutObjValidator putObj_val9 = new PutObjValidator(putObj_req9.id, DMMessage.ResponseCode.SUCCESS);
            builder.add(putObj_req9, putObj_val9);
            
            QueryObj.Req queryObj_req10 = new QueryObj.Req(tidFactory.newId(), typeId2, new ByteId(new byte[]{1,3}), new FieldIs(15), Limit.noLimit());
            Map<ByteId, ByteBuffer> objs10 = new HashMap<ByteId, ByteBuffer>();
            objs10.put(objId2.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj2_type2)));
            objs10.put(objId3.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj3_type2)));
            QueryObjValidator queryObj_val10 = new QueryObjValidator(queryObj_req10.id, DMMessage.ResponseCode.SUCCESS, objs10);
            builder.add(queryObj_req10, queryObj_val10);
            
            QueryObj.Req queryObj_req11 = new QueryObj.Req(tidFactory.newId(), typeId2, new ByteId(new byte[]{1,3}), new FieldIs(15), Limit.noLimit());
            Map<ByteId, ByteBuffer> objs11 = new HashMap<ByteId, ByteBuffer>();
            objs11.put(objId2.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj2_type2)));
            objs11.put(objId3.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj3_type2)));
            QueryObjValidator queryObj_val11 = new QueryObjValidator(queryObj_req11.id, DMMessage.ResponseCode.SUCCESS, objs11);
            builder.add(queryObj_req11, queryObj_val11);
            
            QueryObj.Req queryObj_req12 = new QueryObj.Req(tidFactory.newId(), typeId2, new ByteId(new byte[]{1,3}), new FieldIs(16), Limit.noLimit());
            Map<ByteId, ByteBuffer> objs12 = new HashMap<ByteId, ByteBuffer>();
            objs12.put(objId4.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj4_type2)));
            QueryObjValidator queryObj_val12 = new QueryObjValidator(queryObj_req12.id, DMMessage.ResponseCode.SUCCESS, objs12);
            builder.add(queryObj_req12, queryObj_val12);
            
            QueryObj.Req queryObj_req13 = new QueryObj.Req(tidFactory.newId(), typeId2, new ByteId(new byte[]{1,3}), new FieldScan(14,16), Limit.noLimit());
            Map<ByteId, ByteBuffer> objs13 = new HashMap<ByteId, ByteBuffer>();
            objs13.put(objId2.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj2_type2)));
            objs13.put(objId3.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj3_type2)));
            objs13.put(objId4.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj4_type2)));
            QueryObjValidator queryObj_val13 = new QueryObjValidator(queryObj_req13.id, DMMessage.ResponseCode.SUCCESS, objs13);
            builder.add(queryObj_req13, queryObj_val13);
            
            QueryObj.Req queryObj_req14 = new QueryObj.Req(tidFactory.newId(), typeId2, new ByteId(new byte[]{1,3}), new FieldScan(14,16), Limit.toItems(2));
            Map<ByteId, ByteBuffer> objs14 = new HashMap<ByteId, ByteBuffer>();
            objs14.put(objId2.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj2_type2)));
            objs14.put(objId3.getValue2(), ByteBuffer.wrap(DMSerializer.serialize(obj3_type2)));
//            objs14.put(objId4.getValue2(), DMSerializer.serialize(obj4_type2));
            QueryObjValidator queryObj_val14 = new QueryObjValidator(queryObj_req14.id, DMMessage.ResponseCode.SUCCESS, objs14);
            builder.add(queryObj_req14, queryObj_val14);
            
            return builder.build();
        } catch (TypeInfo.InconsistencyException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    private TypeInfo createType1() {
        TypeInfo.Builder tiBuilder = new TypeInfo.Builder("type1");
        tiBuilder.put(new ByteId(new byte[]{1,1}), new FieldInfo("field1", FieldType.FLOAT, false));
        tiBuilder.put(new ByteId(new byte[]{1,2}), new FieldInfo("field2", FieldType.STRING, false));
        tiBuilder.put(new ByteId(new byte[]{1,3}), new FieldInfo("field3", FieldType.INTEGER, false));
        return tiBuilder.build();
    }
    
    private TypeInfo createType2() {
        TypeInfo.Builder tiBuilder = new TypeInfo.Builder("type1");
        tiBuilder.put(new ByteId(new byte[]{1,1}), new FieldInfo("field1", FieldType.FLOAT, false));
        tiBuilder.put(new ByteId(new byte[]{1,2}), new FieldInfo("field2", FieldType.STRING, false));
        tiBuilder.put(new ByteId(new byte[]{1,3}), new FieldInfo("field3", FieldType.INTEGER, true));
        return tiBuilder.build();
    }
    
    private ObjectValue createObj1_Type1() {
        ObjectValue.Builder ovBuilder = new ObjectValue.Builder();
        ovBuilder.put(new ByteId(new byte[]{1,1}), new Float(17.2));
        ovBuilder.put(new ByteId(new byte[]{1,2}), "test");
        ovBuilder.put(new ByteId(new byte[]{1,3}), new Integer(15));
        return ovBuilder.build();
    }
    
    private ObjectValue createObj2_Type2() {
        ObjectValue.Builder ovBuilder = new ObjectValue.Builder();
        ovBuilder.put(new ByteId(new byte[]{1,1}), new Float(17.2));
        ovBuilder.put(new ByteId(new byte[]{1,2}), "test");
        ovBuilder.put(new ByteId(new byte[]{1,3}), new Integer(15));
        return ovBuilder.build();
    }
    
    private ObjectValue createObj3_Type2() {
        ObjectValue.Builder ovBuilder = new ObjectValue.Builder();
        ovBuilder.put(new ByteId(new byte[]{1,1}), new Float(17.2));
        ovBuilder.put(new ByteId(new byte[]{1,2}), "test2");
        ovBuilder.put(new ByteId(new byte[]{1,3}), new Integer(15));
        return ovBuilder.build();
    }
    
    private ObjectValue createObj4_Type2() {
        ObjectValue.Builder ovBuilder = new ObjectValue.Builder();
        ovBuilder.put(new ByteId(new byte[]{1,1}), new Float(17.2));
        ovBuilder.put(new ByteId(new byte[]{1,2}), "test3");
        ovBuilder.put(new ByteId(new byte[]{1,3}), new Integer(16));
        return ovBuilder.build();
    }
    
    @Override
    public String toString() {
        return "DMExp1";
    }
}
