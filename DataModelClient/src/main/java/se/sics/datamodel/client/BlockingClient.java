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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.Limit.LimitTracker;
import se.sics.caracaldb.utils.TimestampIdFactory;
import se.sics.datamodel.IndexHelper;
import se.sics.datamodel.ObjectValue;
import se.sics.datamodel.QueryType;
import se.sics.datamodel.TypeInfo;
import se.sics.datamodel.client.gson.msg.CGetObj;
import se.sics.datamodel.client.gson.msg.CGetType;
import se.sics.datamodel.client.gson.msg.CPutObj;
import se.sics.datamodel.client.gson.msg.CPutType;
import se.sics.datamodel.client.gson.msg.CQueryObj;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.GetObj;
import se.sics.datamodel.msg.GetType;
import se.sics.datamodel.msg.PutObj;
import se.sics.datamodel.msg.PutType;
import se.sics.datamodel.msg.QueryObj;
import se.sics.datamodel.util.ByteId;

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
        CPutType cputType = CDMSerializer.fromString(putType, CPutType.class);
        return putType(cputType.typeId, cputType.typeInfo);
    }

    public DMMessage.ResponseCode putType(Pair<ByteId, ByteId> typeId, TypeInfo typeInfo) {
        LOG.debug("PutType {}", typeId);

        worker.dataModelTrigger(new PutType.Req(tidFactory.newId(), typeId, CDMSerializer.serialize(typeInfo)));
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
        Pair<DMMessage.ResponseCode, TypeInfo> resp = getType(cgetType.typeId);
        //fix later
        return new GetType.Resp(tidFactory.newId(), resp.getValue0(), cgetType.typeId, CDMSerializer.serialize(resp.getValue1()));
    }

    public Pair<DMMessage.ResponseCode, TypeInfo> getType(Pair<ByteId, ByteId> typeId) {
        worker.dataModelTrigger(new GetType.Req(tidFactory.newId(), typeId));
        try {
            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return Pair.with(DMMessage.ResponseCode.TIMEOUT, null);
            }
            if (resp instanceof GetType.Resp) {
                GetType.Resp getTypeResp = (GetType.Resp) resp;
                if (resp.respCode == DMMessage.ResponseCode.SUCCESS) {
                    TypeInfo typeInfo = CDMSerializer.deserialize(getTypeResp.typeInfo, TypeInfo.class);
                    return Pair.with(resp.respCode, typeInfo);
                } else {
                    return Pair.with(resp.respCode, null);
                }
            }
            throw new RuntimeException("Bad Response Type");
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return Pair.with(DMMessage.ResponseCode.TIMEOUT, null);
        }
    }

    public DMMessage.ResponseCode putObj(String putObj) {
        CPutObj cputObj = CDMSerializer.fromString(putObj, CPutObj.class);
        Pair<DMMessage.ResponseCode, TypeInfo> typeResp = getType(cputObj.objId.removeFrom2());
        if (typeResp.getValue0() == DMMessage.ResponseCode.SUCCESS) {
            return putObj(cputObj.objId, cputObj.getObj(typeResp.getValue1()), typeResp.getValue1());
        }
        return typeResp.getValue0();
    }

    public DMMessage.ResponseCode putObj(Triplet<ByteId, ByteId, ByteId> objId, ObjectValue obj, TypeInfo typeInfo) {
        LOG.debug("PutObj {}", objId);
        try {
            worker.dataModelTrigger(new PutObj.Req(tidFactory.newId(), objId, CDMSerializer.serialize(obj), IndexHelper.getIndexes(typeInfo, obj)));
        } catch (TypeInfo.InconsistencyException ex) {
            throw new RuntimeException(ex);
        }

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
        CGetObj cgetObj = CDMSerializer.fromString(getObj, CGetObj.class);
        Pair<DMMessage.ResponseCode, byte[]> getObjResp = getObj(cgetObj.objId);
        //fix later
        return new GetObj.Resp(tidFactory.newId(), getObjResp.getValue0(), cgetObj.objId, getObjResp.getValue1());
    }

    public Pair<DMMessage.ResponseCode, byte[]> getObj(Triplet<ByteId, ByteId, ByteId> objId) {
        LOG.debug("GetObj {}", objId);
        worker.dataModelTrigger(new GetObj.Req(tidFactory.newId(), objId));
        try {
            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return Pair.with(DMMessage.ResponseCode.TIMEOUT, null);
            }
            if (resp instanceof GetObj.Resp) {
                GetObj.Resp getObjResp = (GetObj.Resp) resp;
                return Pair.with(getObjResp.respCode, getObjResp.value);
            }
            throw new RuntimeException("Bad Response Type");
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return Pair.with(DMMessage.ResponseCode.TIMEOUT, null);
        }
    }
    
    public QueryObj.Resp queryObj(String queryObj) {
        CQueryObj cqueryObj = CDMSerializer.fromString(queryObj, CQueryObj.class);
        Pair<DMMessage.ResponseCode, TypeInfo> typeResp = getType(cqueryObj.typeId);
        if(typeResp.getValue0() == DMMessage.ResponseCode.SUCCESS) {
            QueryType queryType = cqueryObj.indexVal.getQueryType(typeResp.getValue1().get(cqueryObj.indexId).type);
            LimitTracker limit = Limit.noLimit();
            Pair<DMMessage.ResponseCode, Map<ByteId, ByteBuffer>> queryResp = queryObj(cqueryObj.typeId, cqueryObj.indexId, queryType, limit);
            //fix later
            return new QueryObj.Resp(tidFactory.newId(), queryResp.getValue0(), cqueryObj.typeId, queryResp.getValue1());
        }
        return new QueryObj.Resp(tidFactory.newId(), typeResp.getValue0(), cqueryObj.typeId, null);
    }   

    public Pair<DMMessage.ResponseCode, Map<ByteId, ByteBuffer>> queryObj(Pair<ByteId, ByteId> typeId, ByteId indexId, QueryType query, LimitTracker limit) {
        LOG.debug("QueryObj {} indexId {}", typeId, indexId);
        worker.dataModelTrigger(new QueryObj.Req(tidFactory.newId(), typeId, indexId, query, limit));
        try {
            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return Pair.with(DMMessage.ResponseCode.TIMEOUT, null);
            }
            if (resp instanceof QueryObj.Resp) {
                QueryObj.Resp queryResp = (QueryObj.Resp) resp;
                return Pair.with(queryResp.respCode, queryResp.objs);
            }
            throw new RuntimeException("Bad Response Type");
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return Pair.with(DMMessage.ResponseCode.TIMEOUT, null);
        }
    }
}
