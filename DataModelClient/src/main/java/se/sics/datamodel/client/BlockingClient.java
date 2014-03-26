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
package se.sics.datamodel.client;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.client.msg.CGetObj;
import se.sics.datamodel.client.msg.CGetType;
import se.sics.datamodel.client.msg.CPutObj;
import se.sics.datamodel.client.msg.CPutType;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.GetObj;
import se.sics.datamodel.msg.GetType;
import se.sics.datamodel.msg.PutObj;
import se.sics.datamodel.msg.PutType;
import se.sics.datamodel.msg.QueryObj;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class BlockingClient {

    // static
    private static final Logger LOG = LoggerFactory.getLogger(BlockingClient.class);
    private static final long TIMEOUT = 1;
    private static final TimeUnit TIMEUNIT = TimeUnit.SECONDS;

    private final BlockingQueue<DMMessage.Resp> dataModelQueue;
    private final ClientWorker worker;
    private final TimestampIdFactory tidFactory;

    BlockingClient(BlockingQueue<DMMessage.Resp> dataModelQueue, ClientWorker worker) {
        this.dataModelQueue = dataModelQueue;
        this.worker = worker;
        this.tidFactory = TimestampIdFactory.get();
    }

    public DMMessage.ResponseCode putType(String putType) {
        LOG.debug("PutType for {}", putType);
        CPutType cputType = CDMSerializer.fromString(putType, CPutType.class);
        PutType.Req req = cputType.getReq(tidFactory.newId());
        worker.dataModelTrigger(req);
        try {
            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return DMMessage.ResponseCode.TIMEOUT;
            }
            if (resp instanceof PutType.Resp) {
                return ((PutType.Resp) resp).respCode;
            }
            throw new RuntimeException("Bad Response Type");
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return DMMessage.ResponseCode.TIMEOUT;
        }
    }

    public GetType.Resp getType(String getType) {
        LOG.debug("GetType for {}", getType);
        CGetType cgetType = CDMSerializer.fromString(getType, CGetType.class);
        GetType.Req req = cgetType.getReq(tidFactory.newId());
        return getType(req);
    }
    
    private GetType.Resp getType(GetType.Req req) {
        worker.dataModelTrigger(req);
        try {
            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new GetType.Resp(req.id, DMMessage.ResponseCode.TIMEOUT, Pair.with(req.dbId, req.typeId), null);
            }
            if (resp instanceof GetType.Resp) {
                return (GetType.Resp) resp;
            }
            throw new RuntimeException("Bad Response Type");
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new GetType.Resp(req.id, DMMessage.ResponseCode.TIMEOUT, Pair.with(req.dbId, req.typeId), null);
        }
    }

    public DMMessage.ResponseCode putObj(String putObj) {
        LOG.debug("PutObj for {}", putObj);
        CPutObj cputObj = CDMSerializer.fromString(putObj, CPutObj.class);
        GetType.Req typeReq = cputObj.getTypeReq(tidFactory.newId());
        GetType.Resp typeResp = getType(typeReq);
        TypeInfo typeInfo = CDMSerializer.deserialize(typeResp.typeInfo, TypeInfo.class);
        PutObj.Req objReq = cputObj.putObjReq(tidFactory.newId(), typeInfo);

        worker.dataModelTrigger(null);
        try {
            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return DMMessage.ResponseCode.TIMEOUT;
            }
            if (resp instanceof PutObj.Resp) {
                return ((PutObj.Resp) resp).respCode;
            }
            throw new RuntimeException("Bad Response Type");
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return DMMessage.ResponseCode.TIMEOUT;
        }
    }

    public GetObj.Resp getObj(String getObj) {
        LOG.debug("GetObj for {}", getObj);
        CGetObj cgetObj = CDMSerializer.fromString(getObj, CGetObj.class);
        GetObj.Req req = cgetObj.getReq(tidFactory.newId());
        worker.dataModelTrigger(req);
        try {
            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new GetObj.Resp(req.id, DMMessage.ResponseCode.TIMEOUT, Triplet.with(req.dbId, req.typeId, req.objId), null);
            }
            if (resp instanceof GetObj.Resp) {
                return (GetObj.Resp) resp;
            }
            throw new RuntimeException("Bad Response Type");
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new GetObj.Resp(req.id, DMMessage.ResponseCode.TIMEOUT, Triplet.with(req.dbId, req.typeId, req.objId), null);
        }
    }

    public QueryObj.Resp queryObj(String queryObj) {
        throw new UnsupportedOperationException("fix query - easy fix");
//        LOG.debug("QueryObj for {}", queryObj);
//        long id = TimestampIdFactory.get().newId();
//        Gson gson = GsonHelper.getGson();
//        ClientQueryObjGson queryObjGson = gson.fromJson(queryObj, ClientQueryObjGson.class);
//
//        ClientGetTypeGson cl = new ClientGetTypeGson(queryObjGson.dbId, queryObjGson.typeId);
//        GetType.Resp type = getType(gson.toJson(cl));
//        TempTypeInfo typeInfo;
//        try {
//            typeInfo = gson.fromJson(new String(type.typeInfo, "UTF-8"), TempTypeInfo.class);
//        } catch (UnsupportedEncodingException ex) {
//            throw new RuntimeException(ex);
//        }
//        Object indexVal = gson.fromJson(queryObjGson.indexValue, getClass(typeInfo.getField(queryObjGson.indexId).fieldType));
//        QueryObj.Req req = new QueryObj.Req(id, queryObjGson.dbId, queryObjGson.typeId, queryObjGson.indexId, indexVal);
//        worker.dataModelTrigger(req);
//        try {
//            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
//            if (resp == null) {
//                return new QueryObj.Resp(id, DMMessage.ResponseCode.TIMEOUT, null);
//            }
//            if (resp instanceof QueryObj.Resp) {
//                return (QueryObj.Resp) resp;
//            }
//            throw new RuntimeException("Bad Response Type");
//        } catch (InterruptedException ex) {
//            LOG.error("Couldn't get a response.", ex);
//            return new QueryObj.Resp(id, DMMessage.ResponseCode.TIMEOUT, null);
//        }
    }
}