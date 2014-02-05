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
package se.sics.datamodel.operations.primitives;

import java.util.TreeMap;
import org.javatuples.Pair;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.datamodel.operations.DMOperation;
import se.sics.datamodel.operations.DMOperationsManager;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.TFFactory;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMCRQOp extends DMOperation {

    private final DMOperationsManager operationsManager;
    private final RangeQuery.Request req;
    private final RangeQuery.SeqCollector collector;

    public DMCRQOp(long id, DMOperationsManager operationsManager, KeyRange range) {
        super(id);
        this.operationsManager = operationsManager;
        this.req = new RangeQuery.Request(id, range, Limit.noLimit(), TFFactory.noTF(), RangeQuery.Type.SEQUENTIAL);
        this.collector = new RangeQuery.SeqCollector(req);
    }

    //*****DMOperation*****
    @Override
    protected void startHook() {
        LOG.debug("Operation - started", toString());
        operationsManager.send(id, req.id, req);
    }

    @Override
    public void handleMessage(CaracalResponse resp) {
        if(done) {
            LOG.debug("Operation {} - finished and will not process further messages", toString());
            operationsManager.droppedMessage(resp);
            return;
        }
        if (resp instanceof RangeQuery.Response) {
            if (resp.code.equals(ResponseCode.SUCCESS)) {
                collector.processResponse((RangeQuery.Response) resp);
                if (collector.isDone()) {
                    success(collector.getResult());
                }
            } else {
                fail(DMMessage.ResponseCode.FAILURE);
            }
        } else {
            operationsManager.droppedMessage(resp);
        }
    }

    //***** *****
    @Override
    public String toString() {
        return "DM_CRQ " + id;
    }
    
    private void finish(Result result) {
        LOG.debug("Operation {} - finished {}", new Object[]{toString(), result.responseCode});
        done = true;
        operationsManager.childFinished(id, result);
    }

    private void fail(DMMessage.ResponseCode respCode) {
        Result result = new Result(respCode, req.initRange, null, null);
        finish(result);
    }

    private void success(Pair<KeyRange, TreeMap<Key, byte[]>> opResult) {
        Result result = new Result(DMMessage.ResponseCode.SUCCESS, req.initRange, opResult.getValue0(), opResult.getValue1());
        finish(result);
    }
    
    public static class Result extends DMOperation.Result {

        public final KeyRange initRange;
        public final KeyRange coveredRange;
        public final TreeMap<Key, byte[]> results;

        public Result(DMMessage.ResponseCode respCode, KeyRange initRange, KeyRange coveredRange, TreeMap<Key, byte[]> results) {
            super(respCode);
            this.initRange = initRange;
            this.coveredRange = coveredRange;
            this.results = results;
        }

        @Override
        public DMMessage.Resp getMsg(long msgId) {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
    }
}
