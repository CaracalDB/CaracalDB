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

import se.sics.datamodel.operations.primitives.DMPutOp;
import java.io.IOException;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.PutType;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.util.DMKeyFactory;
import se.sics.caracaldb.utils.TimestampIdFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMPutTypeOp extends DMSequentialOp {

    private final Pair<ByteId, ByteId> typeId;
    private final byte[] typeInfo;

    public DMPutTypeOp(long id, DMOperationsManager operationsMaster, Pair<ByteId, ByteId> typeId, byte[] typeInfo) {
        super(id, operationsMaster);
        this.typeId = typeId;
        this.typeInfo = typeInfo;
    }

//    @Override
//    public final void startHook() {
//        LOG.debug("Operation {} - started", toString());
//        ByteId dbId = typeInfo.dbId;
//        ByteId typeId = typeInfo.typeId;
//        TimestampIdFactory tidFactory = TimestampIdFactory.get();
//        for (Map.Entry<ByteId, TempTypeInfo.TempFieldInfo> e : typeInfo.fieldMap.entrySet()) {
//            Key key;
//            byte[] value;
//            
//            try {
//                key = DMKeyFactory.getTMFieldKey(dbId, typeId, e.getKey());
//                value = TempTypeInfo.serializeField(e.getValue());
//            } catch (IOException ex) {
//                fail(DMMessage.ResponseCode.FAILURE);
//                return;
//            }
//            
//            DMOperation pendingOp = new DMPutOp(tidFactory.newId(), this, key, value);
//            pendingOps.put(pendingOp.id, pendingOp);
//            pendingOp.start();
//        }
//    }
    @Override
    public final void startHook() {
        LOG.debug("Operation {} - started", toString());
        TimestampIdFactory tidFactory = TimestampIdFactory.get();
        Key typeKey;
        try {
            typeKey = DMKeyFactory.getTypeKey(typeId.getValue0(), typeId.getValue1());
        } catch (IOException ex) {
            fail(DMMessage.ResponseCode.FAILURE);
            return;
        }
        pendingOp = new DMPutOp(tidFactory.newId(), this, typeKey, typeInfo);
        pendingOp.start();
    }

//    @Override
//    public void childFinished(long opId, DMOperation.Result result) {
//        if (done) {
//            LOG.warn("Operation {} - logical error", toString());
//            return;
//        }
//        if (result instanceof DMPutOp.Result) {
//            if (result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
//                pendingOps.remove(opId);
//                if (pendingOps.isEmpty()) {
//                    success();
//                    return;
//                }
//                return;
//            }
//        }
//        LOG.warn("Operation {} - received unknown child result {}", new Object[]{toString(), result});
//        fail(DMMessage.ResponseCode.FAILURE);
//    }
    @Override
    public void childFinished(long opId, DMOperation.Result result) {
        if (done) {
            LOG.warn("Operation {} - logical error", toString());
            return;
        }
        if (result instanceof DMPutOp.Result) {
            if (result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                success();
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
        return "DM_PUT_TYPE " + id;
    }

    private void fail(DMMessage.ResponseCode respCode) {
        Result result = new Result(respCode);
        finish(result);
    }

    private void success() {
        Result result = new Result(DMMessage.ResponseCode.SUCCESS);
        finish(result);
    }

    public static class Result extends DMOperation.Result {

        public Result(DMMessage.ResponseCode respCode) {
            super(respCode);
        }

        @Override
        public DMMessage.Resp getMsg(long msgId) {
            return new PutType.Resp(msgId, responseCode);
        }
    }
}
