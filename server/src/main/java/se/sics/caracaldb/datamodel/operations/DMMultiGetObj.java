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
package se.sics.caracaldb.datamodel.operations;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.datamodel.util.ByteId;
import se.sics.caracaldb.utils.TimestampIdFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
//rechecked
public class DMMultiGetObj extends DMParallelOp {

    private final ResultBuilder resultBuilder;

    public DMMultiGetObj(long id, DMOperationsManager operationsMaster, ByteId dbId, ByteId typeId, Set<ByteId> objIds) {
        super(id, operationsMaster);
        this.resultBuilder = new ResultBuilder(dbId, typeId, objIds);
    }

    //*****DMOperations*****
    @Override
    public final void startHook() {
        LOG.debug("Operation {}  - started", id);

        TimestampIdFactory tidFactory = TimestampIdFactory.get();
        for (ByteId objId : resultBuilder.objs.keySet()) {
            DMOperation pendingOp = new DMGetObjOp(tidFactory.newId(), this, resultBuilder.dbId, resultBuilder.typeId, objId);
            pendingOps.put(pendingOp.id, pendingOp);
            pendingOp.start();
        }
    }

    @Override
    public final void childFinished(long opId, DMOperation.Result result) {
        if(done) {
            LOG.warn("Operation {} - logical error", toString());
            return;
        }
        if (result instanceof DMGetObjOp.Result) {
            if (result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                DMGetObjOp.Result typedResult = (DMGetObjOp.Result) result;
                resultBuilder.addResult(typedResult.objId, typedResult.value);
                
                cleanChildOp(opId);
                if(pendingOps.isEmpty()) {
                    success();
                    return;
                }
                return;
            }
        }
        LOG.warn("Operation {} - received unknown child result {}", new Object[]{toString(), result});
        fail(DMMessage.ResponseCode.FAILURE);
    }

    //***** *****
    @Override
    public String toString() {
        return "DM_MULTI_GET_OBJ " + id;
    }
    
    private void fail(DMMessage.ResponseCode respCode) {
        Result result = resultBuilder.fail(respCode);
        finish(result);
    }

    private void success() {
        Result result = resultBuilder.build();
        finish(result);
    }
    
    private static class ResultBuilder {
        final ByteId dbId;
        final ByteId typeId;
        final Map<ByteId, byte[]> objs;
        
        ResultBuilder(ByteId dbId, ByteId typeId, Set<ByteId> objIds) {
            this.dbId = dbId;
            this.typeId = typeId;
            this.objs = new TreeMap<ByteId, byte[]>();
            for(ByteId objId : objIds) {
                objs.put(objId, null);
            }
        }
        
        void addResult(ByteId objId, byte[] value) {
            objs.put(objId, value);
        }
        
        Result build() {
            return new Result(DMMessage.ResponseCode.SUCCESS, dbId, typeId, objs);
        }
        
        Result fail(DMMessage.ResponseCode respCode) {
            return new Result(respCode, dbId, typeId, objs);
        }
    }

    public static class Result extends DMOperation.Result {
        public final ByteId dbId;
        public final ByteId typeId;
        public final Map<ByteId, byte[]> values;

        Result(DMMessage.ResponseCode respCode, ByteId dbId, ByteId typeId, Map<ByteId, byte[]> values) {
            super(respCode);
            this.dbId = dbId;
            this.typeId = typeId;
            this.values = values;
        }

        @Override
        public DMMessage.Resp getMsg(long msgId) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
