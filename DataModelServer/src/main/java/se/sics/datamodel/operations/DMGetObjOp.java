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

import se.sics.datamodel.operations.primitives.DMGetOp;
import java.io.IOException;
import org.javatuples.Triplet;
import se.sics.caracaldb.Key;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.GetObj;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.util.DMKeyFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMGetObjOp extends DMSequentialOp {

    private final Triplet<ByteId, ByteId, ByteId> objId;

    public DMGetObjOp(long id, DMOperationsManager operationsMaster, Triplet<ByteId, ByteId, ByteId> objId) {
        super(id, operationsMaster);
        this.objId = objId;
    }

    //*****DMOperations*****
    @Override
    public void startHook() {
        LOG.debug("Operation {} - started", toString());

        Key key;
        try {
            key = DMKeyFactory.getDataKey(objId.getValue0(), objId.getValue1(), objId.getValue2());
        } catch (IOException ex) {
            finish(new Result(DMMessage.ResponseCode.FAILURE, objId, null));
            return;
        }
        pendingOp = new DMGetOp(id, this, key);
        pendingOp.start();
    }

    @Override
    public void childFinished(long opId, DMOperation.Result result) {
        if(done) {
            LOG.warn("Operation {} - logical error", toString());
            return;
        }
        if (result instanceof DMGetOp.Result) {
            if (result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                success(((DMGetOp.Result)result).value);
                return;
            } else {
                fail(DMMessage.ResponseCode.FAILURE);
                return;
            }
        }
        LOG.warn("Operation {} - received unknown child result {}", new Object[]{toString(), result});
        fail(DMMessage.ResponseCode.FAILURE);
    }

    //***** *****
    @Override
    public String toString() {
        return "DM_GET_OBJ " + id;
    }
    
    private void fail(DMMessage.ResponseCode respCode) {
        Result result = new Result(respCode, objId, null);
        finish(result);
    }

    private void success(byte[] value) {
        Result result = new Result(DMMessage.ResponseCode.SUCCESS, objId, value);
        finish(result);
    }
    
    public static class Result extends DMOperation.Result {
        public final Triplet<ByteId, ByteId, ByteId> objId;
        public final byte[] value;
        
        public Result(DMMessage.ResponseCode respCode, Triplet<ByteId, ByteId, ByteId> objId, byte[] value) {
            super(respCode);
            this.objId = objId;
            this.value = value;
        }

        @Override
        public DMMessage.Resp getMsg(long msgId) {
            return new GetObj.Resp(msgId, responseCode, objId, value);
        }
    }
}
