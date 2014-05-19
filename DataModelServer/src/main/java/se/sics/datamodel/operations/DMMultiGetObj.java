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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.javatuples.Pair;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.util.ByteId;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
//rechecked
public class DMMultiGetObj extends DMParallelOp {

    private final ResultBuilder resultBuilder;

    public DMMultiGetObj(UUID id, DMOperationsManager operationsMaster, Pair<ByteId, ByteId> typeId, Set<ByteId> objIds) {
        super(id, operationsMaster);
        this.resultBuilder = new ResultBuilder(typeId, objIds);
    }

    //*****DMOperations*****
    @Override
    public final void startHook() {
        LOG.debug("Operation {}  - started", toString());

        TimestampIdFactory tidFactory = TimestampIdFactory.get();
        List<UUID> ids = tidFactory.newIds(resultBuilder.objs.keySet().size()).asList();
        int i = 0;
        for (ByteId objId : resultBuilder.objs.keySet()) {
            DMOperation pendingOp = new DMGetObjOp(ids.get(i), this, resultBuilder.typeId.add(objId));
            pendingOps.put(pendingOp.id, pendingOp);
            pendingOp.start();
            i++;
        }

        if (pendingOps.isEmpty()) {
            success();
        }
    }

    @Override
    public final void childFinished(UUID opId, DMOperation.Result result) {
        if (done) {
            LOG.warn("Operation {} - logical error", toString());
            return;
        }
        if (result instanceof DMGetObjOp.Result) {
            if (result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                DMGetObjOp.Result typedResult = (DMGetObjOp.Result) result;
                resultBuilder.addResult(typedResult.objId.getValue2(), typedResult.value);
                cleanChildOp(opId);
                if (pendingOps.isEmpty()) {
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

        final Pair<ByteId, ByteId> typeId;
        final Map<ByteId, ByteBuffer> objs;

        ResultBuilder(Pair<ByteId, ByteId> typeId, Set<ByteId> objIds) {
            this.typeId = typeId;
            this.objs = new TreeMap<ByteId, ByteBuffer>();
            for (ByteId objId : objIds) {
                objs.put(objId, null);
            }
        }

        void addResult(ByteId objId, byte[] value) {
            objs.put(objId, ByteBuffer.wrap(value));
        }

        Result build() {
            return new Result(DMMessage.ResponseCode.SUCCESS, typeId, objs);
        }

        Result fail(DMMessage.ResponseCode respCode) {
            return new Result(respCode, typeId, objs);
        }
    }

    public static class Result extends DMOperation.Result {

        public final Pair<ByteId, ByteId> typeId;
        public final Map<ByteId, ByteBuffer> values;

        Result(DMMessage.ResponseCode respCode, Pair<ByteId, ByteId> typeId, Map<ByteId, ByteBuffer> values) {
            super(respCode);
            this.typeId = typeId;
            this.values = values;
        }

        @Override
        public DMMessage.Resp getMsg(UUID msgId) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
