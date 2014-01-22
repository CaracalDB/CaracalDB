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
package se.sics.caracaldb.simulation.operations.datamodel.cmd;

import com.google.gson.Gson;
import java.io.UnsupportedEncodingException;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.datamodel.msg.GetType;
import se.sics.caracaldb.datamodel.msg.PutType;
import se.sics.caracaldb.datamodel.util.ByteId;
import se.sics.caracaldb.datamodel.util.ByteIdFactory;
import se.sics.caracaldb.datamodel.util.FieldInfo;
import se.sics.caracaldb.datamodel.util.GsonHelper;
import se.sics.caracaldb.datamodel.util.TempTypeInfo;
import se.sics.caracaldb.simulation.operations.datamodel.DMExperiment;
import se.sics.caracaldb.simulation.operations.datamodel.validators.GetTypeValidator;
import se.sics.caracaldb.simulation.operations.datamodel.validators.PutTypeValidator;
import se.sics.caracaldb.utils.TimestampIdFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */


public class DMExp1Cmd extends DMExpCmd {
    @Override
    public DMExperiment getExp() {
        Gson gson = GsonHelper.getGson();
        
        DMExperiment.Builder builder = new DMExperiment.Builder();
        TimestampIdFactory tidFactory = TimestampIdFactory.get();
        ByteIdFactory bidFactory = new ByteIdFactory();
        
        GetType.Req getType_req1 = new GetType.Req(tidFactory.newId(), new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,1}));
        GetTypeValidator getType_val1 = new GetTypeValidator(getType_req1.id, DMMessage.ResponseCode.FAILURE, null);
        builder.add(getType_req1, getType_val1);
        
        TempTypeInfo typeInfo = createType_1(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,1}));
        byte[] byteTypeInfo;
        try {
            byteTypeInfo = gson.toJson(typeInfo).getBytes("UTF-8");
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
        PutType.Req putType_req2 = new PutType.Req(tidFactory.newId(), typeInfo.dbId, typeInfo.typeId, byteTypeInfo);
        PutTypeValidator putType_val2 = new PutTypeValidator(putType_req2.id, DMMessage.ResponseCode.SUCCESS);
        builder.add(getType_req1, getType_val1);
        
        GetType.Req getType_req3 = new GetType.Req(tidFactory.newId(), new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,1}));
        GetTypeValidator getType_val3 = new GetTypeValidator(getType_req1.id, DMMessage.ResponseCode.SUCCESS, byteTypeInfo);
        builder.add(getType_req3, getType_val3);
        
        return builder.build();
    }
    
    private TempTypeInfo createType_1(ByteId dbId, ByteId typeId) {
        TempTypeInfo typeInfo = new TempTypeInfo(dbId, typeId);
        typeInfo.addField("field1", FieldInfo.FieldType.FLOAT, false);
        typeInfo.addField("field2", FieldInfo.FieldType.STRING, false);
        typeInfo.addField("field3", FieldInfo.FieldType.INTEGER, true);
        return typeInfo;
    }
}
