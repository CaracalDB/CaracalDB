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
import se.sics.caracaldb.Key;
import se.sics.caracaldb.datamodel.DataModel;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.datamodel.operations.DMOperation;
import se.sics.caracaldb.datamodel.operations.DMOperationsMaster;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.PutResponse;
import se.sics.caracaldb.operations.ResponseCode;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMPutOp extends DMOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DataModel.class);

    private final DMOperationsMaster operationsMaster;
    private final PutRequest req;

    public DMPutOp(long id, DMOperationsMaster operationsMaster, Key key, byte[] value) {
        super(id);
        this.operationsMaster = operationsMaster;
        this.req = new PutRequest(id, key, value);
    }

    @Override
    public void start() {
        LOG.debug("Operation {} DM_PUT - started", id);
        operationsMaster.send(id, req.id, req);
    }

    @Override
    public void handleMessage(CaracalResponse resp) {
        if (resp instanceof PutResponse) {
            if (resp.code.equals(ResponseCode.SUCCESS)) {
                Result result = new Result(DMMessage.ResponseCode.SUCCESS);
                finish(result);
            } else {
                Result result = new Result(DMMessage.ResponseCode.FAILURE);
                finish(result);
            }
        } else {
            operationsMaster.droppedMessage(resp);
        }
    }
    
    //***** *****
    private void finish(Result result) {
        LOG.debug("Operation {} DM_PUT - finished {}", new Object[]{id, result.responseCode});
        operationsMaster.childFinished(id, result);
    }

    public static class Result extends DMOperation.Result {
        public Result(DMMessage.ResponseCode respCode) {
            super(respCode);
        }
    }
}
