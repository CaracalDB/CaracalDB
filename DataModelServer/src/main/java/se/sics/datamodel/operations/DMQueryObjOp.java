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
package se.sics.datamodel.operations;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.store.Limit.LimitTracker;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.QueryObj;
import se.sics.datamodel.operations.primitives.DMCRQOp;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.util.DMKeyFactory;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.datamodel.QueryType;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMQueryObjOp extends DMSequentialOp {
    private final Pair<ByteId, ByteId>  typeId;
    private final ByteId indexId;
    private final QueryType indexVal;
    private final LimitTracker limit;
    
    public DMQueryObjOp(long id, DMOperationsManager operationsMaster, Pair<ByteId, ByteId> typeId, ByteId indexId, QueryType indexValue, LimitTracker limit) {
        super(id, operationsMaster);
        this.typeId = typeId;
        this.indexId = indexId;
        this.indexVal = indexValue;
        this.limit = limit;
    }
    
    //*****DMOperations*****
    @Override 
    public void startHook() {
        LOG.debug("Operation {} - started", toString());
        TimestampIdFactory tidFactory = TimestampIdFactory.get();
        
        KeyRange indexRange;
        try {
            indexRange = indexVal.getIndexRange(typeId, indexId);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        pendingOp = new DMCRQOp(tidFactory.newId(), this, indexRange, limit);
        pendingOp.start();
    }
    
    @Override
    public void childFinished(long opId, DMOperation.Result result) {
        if(done) {
            LOG.warn("Operation {} - logical error", toString());
            return;
        }
        if(result instanceof DMCRQOp.Result && opId == pendingOp.id) {
            if(result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                TimestampIdFactory tidFactory = TimestampIdFactory.get();
                DMCRQOp.Result typedResult = (DMCRQOp.Result) result;
                pendingOp = new DMMultiGetObj(tidFactory.newId(), this, typeId, getObjectIds(typedResult.results.keySet()));
                pendingOp.start();
                return;
            } else {
                fail(result.responseCode);
                return;
            }
        } else if (result instanceof DMMultiGetObj.Result && opId == pendingOp.id) {
            if(result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                success(((DMMultiGetObj.Result)result).values);
                return;
            } else {
                fail(result.responseCode);
                return;
            }
        }
        LOG.warn("Operation {} - received unknown child result {}", new Object[]{toString(), result});
        fail(DMMessage.ResponseCode.FAILURE);
    }
    
    //***** *****
    @Override
    public String toString() {
        return "M_QUERY_OBJ " + id;
    }
    
    private void fail(DMMessage.ResponseCode respCode) {
        Result result = new Result(respCode, typeId, null);
        finish(result);
    }
    
    private void success(Map<ByteId, byte[]> objs) {
        Result result = new Result(DMMessage.ResponseCode.SUCCESS, typeId, objs);
        finish(result);
    }
    
    private Set<ByteId> getObjectIds(Set<Key> indexKeys) {
        Set<ByteId> objectIds = new HashSet<ByteId>();
        for(Key key : indexKeys) {
            DMKeyFactory.DMKeyComponents keyComp;
            try {
                keyComp = DMKeyFactory.getKeyComponents(key);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            if(!(keyComp instanceof DMKeyFactory.IndexKeyComp)) {
                throw new RuntimeException();
            }
            DMKeyFactory.IndexKeyComp indexKeyComp = (DMKeyFactory.IndexKeyComp)keyComp;
            objectIds.add(indexKeyComp.objId);
        }
        
        return objectIds;
    }
    
    public static class Result extends DMOperation.Result {
        public final Pair<ByteId, ByteId> typeId;
        public final Map<ByteId, byte[]> objs;
        
        public Result(DMMessage.ResponseCode respCode, Pair<ByteId, ByteId> typeId, Map<ByteId, byte[]> objs) {
            super(respCode);
            this.typeId = typeId;
            this.objs = objs;
        }
        
        @Override
        public DMMessage.Resp getMsg(long msgId) {
            return new QueryObj.Resp(msgId, responseCode, typeId, objs);
        }
    }
}
