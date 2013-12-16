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

import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.datamodel.DataModel;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.TFFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMCRQOp extends DMOperation {

    private static final Logger LOG = LoggerFactory.getLogger(DataModel.class);

    private final DMOperationsMaster operationsMaster;
    private final RangeQuery.Request req;
    private final RangeQuery.SeqCollector collector;

    public DMCRQOp(long id, DMOperationsMaster operationsMaster, KeyRange range) {
        super(id);
        this.operationsMaster = operationsMaster;
        this.req = new RangeQuery.Request(id, range, Limit.noLimit(), TFFactory.noTF(), RangeQuery.Type.SEQUENTIAL);
        collector = new RangeQuery.SeqCollector(req);
    }

    //*****OMOperation*****
    @Override
    public void start() {
        LOG.debug("Operation {} DM_CRQ - started", id);
        operationsMaster.send(id, req.id, req);
    }

    @Override
    public void handleMessage(CaracalResponse resp) {
        if (resp instanceof RangeQuery.Response) {
            if(resp.code.equals(ResponseCode.SUCCESS)) {
                collector.processResponse((RangeQuery.Response) resp);
                if(collector.isDone()) {
                    Result result = new Result(DMMessage.ResponseCode.SUCCESS, collector.getResult());
                    finish(result);
                }
            } else {
                Result result = new Result(DMMessage.ResponseCode.FAILURE, null);
                finish(result);
            }
        } else {
            operationsMaster.droppedMessage(resp);
        }
    }

    //***** *****
    private void finish(Result result) {
        LOG.debug("Operation {} DM_CRQ - finished {}", new Object[]{id, result.responseCode});
        operationsMaster.childFinished(id, result);
    }
    
    public static class Result extends DMOperation.Result {
        public final TreeMap<Key, byte[]> results;
        
        public Result(DMMessage.ResponseCode respCode, TreeMap<Key, byte[]> results) {
            super(respCode);
            this.results = results;
        }
    }
}
