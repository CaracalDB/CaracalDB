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

import se.sics.datamodel.operations.primitives.DMCRQOp;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.datamodel.msg.GetAllTypes;
import se.sics.caracaldb.datamodel.util.ByteId;
import se.sics.caracaldb.datamodel.util.DMKeyFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMGetAllTypesOp extends DMSequentialOp {

    private final ByteId dbId;

    public DMGetAllTypesOp(long id, DMOperationsManager operationsManager, ByteId dbId) {
        super(id, operationsManager);
        this.dbId = dbId;
    }

    //*****DMOperation*****
    @Override
    public final void startHook() {
        LOG.debug("Operation {} - started", toString());
        KeyRange allTypesRange;
        try {
            allTypesRange = DMKeyFactory.getAllTypesRange(dbId);
        } catch (IOException ex) {
            fail(DMMessage.ResponseCode.FAILURE);
            return;
        }
        pendingOp = new DMCRQOp(id, this, allTypesRange);
        pendingOp.start();
    }

    @Override
    public final void childFinished(long opId, DMOperation.Result result) {
        if (done) {
            LOG.warn("Operation {} - logical error", toString());
            return;
        }
        if (result instanceof DMCRQOp.Result) {
            if (result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                TreeMap<Key, byte[]> b_results = ((DMCRQOp.Result) result).results;
                TreeMap<String, ByteId> results = new TreeMap<String, ByteId>();
                for (Map.Entry<Key, byte[]> e : b_results.entrySet()) {
                    try {
                        DMKeyFactory.TypeKeyComp comp = (DMKeyFactory.TypeKeyComp) DMKeyFactory.getKeyComponents(e.getKey());
                        String typeName = new String(e.getValue(), "UTF8");
                        results.put(typeName, comp.typeId);
                    } catch (IOException ex) {
                        fail(DMMessage.ResponseCode.FAILURE);
                        return;
                    }
                }
                success(results);
                return;
            }
        }
        LOG.warn("Operation {} - received unknown child result {}", new Object[]{toString(), result});
        fail(DMMessage.ResponseCode.FAILURE);
    }

    //***** *****
    @Override
    public String toString() {
        return "DM_GET_ALLTYPES " + id;
    }

    private void fail(DMMessage.ResponseCode respCode) {
        Result result = new Result(respCode, dbId, null);
        finish(result);
    }

    private void success(TreeMap<String, ByteId> types) {
        Result result = new Result(DMMessage.ResponseCode.SUCCESS, dbId, types);
        finish(result);
    }

    public static class Result extends DMOperation.Result {

        public final ByteId dbId;
        public final TreeMap<String, ByteId> types;

        public Result(DMMessage.ResponseCode respCode, ByteId dbId, TreeMap<String, ByteId> types) {
            super(respCode);
            this.dbId = dbId;
            this.types = types;
        }

        @Override
        public DMMessage.Resp getMsg(long msgId) {
            return new GetAllTypes.Resp(msgId, responseCode, dbId, types);
        }
    }
}
