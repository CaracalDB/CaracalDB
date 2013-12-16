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
package se.sics.caracaldb.datamodel;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.datamodel.msg.GetAllTypes;
import se.sics.caracaldb.datamodel.msg.GetGsonObj;
import se.sics.caracaldb.datamodel.msg.GetType;
import se.sics.caracaldb.datamodel.msg.PutGsonObj;
import se.sics.caracaldb.datamodel.msg.PutType;
import se.sics.caracaldb.datamodel.operations.DMGetAllTypesOp;
import se.sics.caracaldb.datamodel.operations.DMGetObjOp;
import se.sics.caracaldb.datamodel.operations.DMGetTypeOp;
import se.sics.caracaldb.datamodel.operations.DMOperation;
import se.sics.caracaldb.datamodel.operations.DMOperationsManager;
import se.sics.caracaldb.datamodel.operations.DMPutObjOp;
import se.sics.caracaldb.datamodel.operations.DMPutTypeOp;
import se.sics.caracaldb.operations.CaracalOp;
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
        subscribe(getGsonObjHandler, dataModel);
        subscribe(putGsonObjHandler, dataModel);
    }

    Handler<GetAllTypes.Req> getAllTypesHandler = new Handler<GetAllTypes.Req>() {

        @Override
        public void handle(GetAllTypes.Req req) {
            long opId = tidFactory.newId();
            DMOperation op = new DMGetAllTypesOp(opId, operationsManager, req.dbId);
            pendingOps.put(opId, op);
            op.start();
        }

    };

    Handler<GetType.Req> getTypeHandler = new Handler<GetType.Req>() {

        @Override
        public void handle(GetType.Req req) {
            long opId = tidFactory.newId();
            DMOperation op = new DMGetTypeOp(opId, operationsManager, req.dbId, req.typeId);
            pendingOps.put(opId, op);
            op.start();
        }

    };

    Handler<PutType.Req> putTypeHandler = new Handler<PutType.Req>() {

        @Override
        public void handle(PutType.Req req) {
            long opId = tidFactory.newId();
            DMOperation op = new DMPutTypeOp(opId, operationsManager, req.typeInfo);
            pendingOps.put(opId, op);
            op.start();
        }

    };

    Handler<GetGsonObj.Req> getGsonObjHandler = new Handler<GetGsonObj.Req>() {

        @Override
        public void handle(GetGsonObj.Req req) {
            long opId = tidFactory.newId();
            DMOperation op = new DMGetObjOp(opId, operationsManager, req.dbId, req.typeId, req.objId);
            pendingOps.put(opId, op);
            op.start();
        }

    };

    Handler<PutGsonObj.Req> putGsonObjHandler = new Handler<PutGsonObj.Req>() {

        @Override
        public void handle(PutGsonObj.Req req) {
            long opId = tidFactory.newId();
            DMOperation op = new DMPutObjOp(opId, operationsManager, req.dbId, req.typeId, req.objId, req.value);
            pendingOps.put(opId, op);
            op.start();
        }

    };

    //*****DMOperationsManager*****
    @Override
    public void send(long opId, long reqId, CaracalOp req) {
        pendingReqs.put(reqId, opId);
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
        while(it.hasNext()) {
            Map.Entry<Long, Long> e = it.next();
            if(e.getValue() == opId) {
                it.remove();
            }
        }
    }
}
