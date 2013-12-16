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
import se.sics.caracaldb.datamodel.util.TempTypeInfo;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMGetTypeOp extends DMSequentialOp {

    private static final Logger LOG = LoggerFactory.getLogger(DataModel.class);

    private final ByteId dbId;
    private final ByteId typeId;

    public DMGetTypeOp(long id, DMOperationsMaster operationsMaster, ByteId dbId, ByteId typeId) {
        super(id, operationsMaster);
        this.dbId = dbId;
        this.typeId = typeId;
    }

    @Override
    public void start() {
        LOG.debug("Operation {} DM_GET_TYPE - started", id);

        KeyRange range;
        try {
            range = DMKeyFactory.getTMRange(dbId, typeId);
        } catch (IOException ex) {
            finish(new Result(DMMessage.ResponseCode.FAILURE, null));
            return;
        }
        pendingOp = new DMCRQOp(id, operationsMaster, range);
        pendingOp.start();
    }

    @Override
    public void childFinished(long opId, DMOperation.Result result) {
        if (result instanceof DMCRQOp.Result) {
            if (result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                TreeMap<Key, byte[]> b_results = ((DMCRQOp.Result) result).results;
                TempTypeInfo typeInfo = new TempTypeInfo(dbId, typeId);

                for (Map.Entry<Key, byte[]> e : b_results.entrySet()) {
                    try {
                        DMKeyFactory.DMKeyComponents comp = DMKeyFactory.getKeyComponents(e.getKey());
                        if (comp instanceof DMKeyFactory.TMFieldKeyComp) {
                            typeInfo.deserializeField(e.getValue());
                        } else {
                            finish(new Result(DMMessage.ResponseCode.FAILURE, null));
                            return;
                        }
                    } catch (IOException ex) {
                        finish(new Result(DMMessage.ResponseCode.FAILURE, null));
                        return;
                    }
                }
                finish(new Result(DMMessage.ResponseCode.SUCCESS, typeInfo));
                return;
            }
        }
        finish(new Result(DMMessage.ResponseCode.FAILURE, null));
    }

    //***** *****
    private void finish(Result result) {
        LOG.debug("Operation {} DM_GETTYPE - finished {}", new Object[]{id, result.responseCode});
        operationsMaster.childFinished(id, result);
    }

    public static class Result extends DMOperation.Result {

        public final TempTypeInfo typeInfo;

        public Result(DMMessage.ResponseCode respCode, TempTypeInfo typeInfo) {
            super(respCode);
            this.typeInfo = typeInfo;
        }
    }
}
