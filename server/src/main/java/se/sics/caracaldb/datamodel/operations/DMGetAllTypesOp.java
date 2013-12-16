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

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.datamodel.DataModel;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.datamodel.util.ByteId;
import se.sics.caracaldb.datamodel.util.DMKeyFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMGetAllTypesOp extends DMSequentialOp {

    private static final Logger LOG = LoggerFactory.getLogger(DataModel.class);

    private final ByteId dbId;

    public DMGetAllTypesOp(long id, DMOperationsMaster operationsMaster, ByteId dbId) {
        super(id, operationsMaster);
        this.dbId = dbId;
    }

    //*****DMOperation*****
    @Override
    public void start() {
        LOG.debug("Operation {} DM_GET_ALLTYPES - started", id);
        KeyRange allTypesRange;
        try {
            allTypesRange = DMKeyFactory.getAllTypesRange(dbId);
        } catch (IOException ex) {
            finish(new Result(DMMessage.ResponseCode.FAILURE, null));
            return;
        }
        pendingOp = new DMCRQOp(id, this, allTypesRange);
        pendingOp.start();
    }

    @Override
    public void childFinished(long opId, DMOperation.Result result) {
        if (result instanceof DMCRQOp.Result) {
            if (result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                TreeMap<Key, byte[]> b_results = ((DMCRQOp.Result) result).results;
                TreeMap<String, ByteId> results = new TreeMap<String, ByteId>();
                for (Map.Entry<Key, byte[]> e : b_results.entrySet()) {
                    try {
                        DMKeyFactory.TypeKeyComp comp = (DMKeyFactory.TypeKeyComp)DMKeyFactory.getKeyComponents(e.getKey());
                        String typeName = new String(e.getValue(), "UTF8");
                        results.put(typeName, comp.typeId);
                    } catch (IOException ex) {
                        finish(new Result(DMMessage.ResponseCode.FAILURE, null));
                        return;
                    }
                }
                finish(new Result(DMMessage.ResponseCode.SUCCESS, results));
                return;
            }
        }
        finish(new Result(DMMessage.ResponseCode.FAILURE, null));
    }

    //***** *****
    private void finish(Result result) {
        LOG.debug("Operation {} DM_GETALLTYPES - finished {}", new Object[]{id, result.responseCode});
        operationsMaster.childFinished(id, result);
    }

    public static class Result extends DMOperation.Result {

        public final TreeMap<String, ByteId> types;

        public Result(DMMessage.ResponseCode respCode, TreeMap<String, ByteId> types) {
            super(respCode);
            this.types = types;
        }
    }
}
