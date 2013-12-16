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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.datamodel.DataModel;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */


public class DMPutObjOp extends DMSequentialOp {

    private static final Logger LOG = LoggerFactory.getLogger(DataModel.class);

    private final ByteId dbId;
    private final ByteId typeId;
    private final ByteId objId;
    private final byte[] b_obj;

    public DMPutObjOp(long id, DMOperationsMaster operationsMaster, ByteId dbId, ByteId typeId, ByteId objId) {
        super(id, operationsMaster);
        this.dbId = dbId;
        this.typeId = typeId;
        this.objId = objId;
    }

    //*****DMOperations*****
    @Override
    public void start() {
        LOG.debug("Operation {} DM_GETOBJ - started", id);

        Key key;
        try {
            key = DMKeyFactory.getDataKey(dbId, typeId, objId);
        } catch (IOException ex) {
            finish(new Result(DMMessage.ResponseCode.FAILURE, null));
            return;
        }
        pendingOp = new DMGetOp(id, operationsMaster, key);
        pendingOp.start();
    }

    @Override
    public void childFinished(long opId, DMOperation.Result result) {
        if (result instanceof DMGetOp.Result) {
            if (result.responseCode.equals(DMMessage.ResponseCode.SUCCESS)) {
                finish(new Result(DMMessage.ResponseCode.SUCCESS, ((DMGetOp.Result)result).value));
                return;
            }
        }
        finish(new Result(DMMessage.ResponseCode.FAILURE, null));
    }

    //***** *****
    private void finish(Result result) {
        LOG.debug("Operation {} DM_GETOBJ - finished {}", new Object[]{id, result.responseCode});
        operationsMaster.childFinished(id, result);
    }

    public static class Result extends DMOperation.Result {

        public final byte[] b_obj;
        
        public Result(DMMessage.ResponseCode respCode, byte[] b_obj) {
            super(respCode);
            this.b_obj = b_obj;
        }
    }
}
