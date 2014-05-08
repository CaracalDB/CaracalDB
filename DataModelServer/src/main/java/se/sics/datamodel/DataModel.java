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
package se.sics.datamodel;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.datamodel.msg.GetAllTypes;
import se.sics.datamodel.msg.GetObj;
import se.sics.datamodel.msg.GetType;
import se.sics.datamodel.msg.PutObj;
import se.sics.datamodel.msg.PutType;
import se.sics.datamodel.msg.QueryObj;
import se.sics.datamodel.operations.DMGetAllTypesOp;
import se.sics.datamodel.operations.DMGetObjOp;
import se.sics.datamodel.operations.DMGetTypeOp;
import se.sics.datamodel.operations.DMOperation;
import se.sics.datamodel.operations.DMOperationsManager;
import se.sics.datamodel.operations.DMPutObjOp;
import se.sics.datamodel.operations.DMPutTypeOp;
import se.sics.datamodel.operations.DMQueryObjOp;
import se.sics.caracaldb.operations.CaracalOp;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.kompics.ComponentDefinition;
import se.sics.kompics.Handler;
import se.sics.kompics.Negative;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DataModel extends ComponentDefinition implements DMOperationsManager {

    private static final Logger LOG = LoggerFactory.getLogger(DataModel.class);

    Negative<DataModelPort> dataModel = provides(DataModelPort.class);

    private Map<Long, DMOperation> pendingOps; //<opId, op>
    private Map<Long, Long> pendingReqs; //<reqId, opId>
    private TimestampIdFactory tidFactory;
    private DMOperationsManager operationsManager;

    public DataModel(DataModelInit init) {
        tidFactory = TimestampIdFactory.get();
        operationsManager = this;
        pendingOps = new HashMap<Long, DMOperation>();
        pendingReqs = new HashMap<Long, Long>();

//        subscribe(requestHandler, dataModel);
        subscribe(getAllTypesHandler, dataModel);
        subscribe(getTypeHandler, dataModel);
        subscribe(putTypeHandler, dataModel);
        subscribe(getObjHandler, dataModel);
        subscribe(putObjHandler, dataModel);
        subscribe(queryObjHandler, dataModel);

        subscribe(caracalResponseHandler, dataModel);
    }

    Handler<GetAllTypes.Req> getAllTypesHandler = new Handler<GetAllTypes.Req>() {

        @Override
        public void handle(GetAllTypes.Req req) {
            DMOperation op = new DMGetAllTypesOp(req.id, operationsManager, req.dbId);
            pendingOps.put(op.id, op);
            op.start();
        }

    };

    Handler<GetType.Req> getTypeHandler = new Handler<GetType.Req>() {

        @Override
        public void handle(GetType.Req req) {
            DMOperation op = new DMGetTypeOp(req.id, operationsManager, Pair.with(req.dbId,req.typeId));
            pendingOps.put(op.id, op);
            op.start();
        }

    };

    Handler<PutType.Req> putTypeHandler = new Handler<PutType.Req>() {

        @Override
        public void handle(PutType.Req req) {
            DMOperation op = new DMPutTypeOp(req.id, operationsManager, Pair.with(req.dbId, req.typeId), req.typeInfo);
            pendingOps.put(op.id, op);
            op.start();
        }

    };

    Handler<GetObj.Req> getObjHandler = new Handler<GetObj.Req>() {

        @Override
        public void handle(GetObj.Req req) {
            DMOperation op = new DMGetObjOp(req.id, operationsManager, Triplet.with(req.dbId, req.typeId, req.objId));
            pendingOps.put(op.id, op);
            op.start();
        }

    };

    Handler<PutObj.Req> putObjHandler = new Handler<PutObj.Req>() {

        @Override
        public void handle(PutObj.Req req) {
            DMOperation op = new DMPutObjOp(req.id, operationsManager, Triplet.with(req.dbId, req.typeId, req.objId), req.objValue, req.indexValue);
            pendingOps.put(op.id, op);
            op.start();
        }

    };
    
    Handler<QueryObj.Req> queryObjHandler = new Handler<QueryObj.Req>() {

        @Override
        public void handle(QueryObj.Req req) {
            DMOperation op = new DMQueryObjOp(req.id, operationsManager, Pair.with(req.dbId, req.typeId), req.indexId, req.indexVal, req.limit);
            pendingOps.put(op.id, op);
            op.start();
        }
        
    };

    Handler<CaracalResponse> caracalResponseHandler = new Handler<CaracalResponse>() {

        @Override
        public void handle(CaracalResponse event) {
            Long opId = pendingReqs.remove(event.id);
            if(opId == null) {
                LOG.warn("opId null - investigate");
                return;
            }
            DMOperation op = pendingOps.get(opId);
            op.handleMessage(event);
        }

    };

    //*****DMOperationsManager*****
    @Override
    public void send(long opId, long reqId, CaracalOp req) {
        LOG.debug("sending " + req.toString());
        pendingReqs.put(reqId, opId);
        trigger(req, dataModel);
    }

    @Override
    public void childFinished(long opId, DMOperation.Result opResult) {
        trigger(opResult.getMsg(opId), dataModel);
        cleanOp(opId);
    }

    @Override
    public void droppedMessage(CaracalOp resp) {
        LOG.warn("Dropping message {}", resp);
    }

    private void cleanOp(long opId) {
        pendingOps.remove(opId);
        Iterator<Map.Entry<Long, Long>> it = pendingReqs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            if (e.getValue() == opId) {
                it.remove();
            }
        }
    }
}
