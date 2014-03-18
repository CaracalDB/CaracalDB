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
package se.sics.caracaldb.client;

import com.google.gson.Gson;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;
import se.sics.caracaldb.datamodel.msg.DMMessage;
import se.sics.caracaldb.datamodel.msg.GetObj;
import se.sics.caracaldb.datamodel.msg.GetType;
import se.sics.caracaldb.datamodel.msg.PutObj;
import se.sics.caracaldb.datamodel.msg.PutType;
import se.sics.caracaldb.datamodel.msg.QueryObj;
import se.sics.caracaldb.datamodel.util.ByteId;
import se.sics.caracaldb.datamodel.util.FieldInfo;
import se.sics.caracaldb.datamodel.util.GsonHelper;
import se.sics.caracaldb.datamodel.util.TempObject;
import se.sics.caracaldb.datamodel.util.TempTypeInfo;
import se.sics.caracaldb.datamodel.util.gsonextra.ClientGetObjGson;
import se.sics.caracaldb.datamodel.util.gsonextra.ClientGetTypeGson;
import se.sics.caracaldb.datamodel.util.gsonextra.ClientPutObjGson;
import se.sics.caracaldb.datamodel.util.gsonextra.ClientQueryObjGson;
import se.sics.caracaldb.operations.CaracalResponse;
import se.sics.caracaldb.operations.GetRequest;
import se.sics.caracaldb.operations.GetResponse;
import se.sics.caracaldb.operations.PutRequest;
import se.sics.caracaldb.operations.RangeQuery;
import se.sics.caracaldb.operations.RangeResponse;
import se.sics.caracaldb.operations.ResponseCode;
import se.sics.caracaldb.store.Limit;
import se.sics.caracaldb.store.Limit.LimitTracker;
import se.sics.caracaldb.store.TFFactory;
import se.sics.caracaldb.utils.TimestampIdFactory;

/**
 *
 * @author Lars Kroll <lkroll@sics.se>
 */
public class BlockingClient {

    // static
    private static final Logger LOG = LoggerFactory.getLogger(BlockingClient.class);
    private static final long TIMEOUT = 1;
    private static final TimeUnit TIMEUNIT = TimeUnit.SECONDS;
    // instance
    private final BlockingQueue<CaracalResponse> responseQueue;
    private final BlockingQueue<DMMessage.Resp> dataModelQueue;
    private final ClientWorker worker;

    private final Map<ByteId, TempTypeInfo> types;

    BlockingClient(BlockingQueue<CaracalResponse> responseQueue, BlockingQueue<DMMessage.Resp> dataModelQueue, ClientWorker worker) {
        this.responseQueue = responseQueue;
        this.dataModelQueue = dataModelQueue;
        this.worker = worker;
        this.types = new HashMap<ByteId, TempTypeInfo>();
    }

    public ResponseCode put(Key k, byte[] value) {
        LOG.debug("Putting on {}", k);
        PutRequest req = new PutRequest(TimestampIdFactory.get().newId(), k, value);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return ResponseCode.CLIENT_TIMEOUT;
            }
            return resp.code;
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return ResponseCode.CLIENT_TIMEOUT;
        }
    }

    public GetResponse get(Key k) {
        LOG.debug("Getting for {}", k);
        long id = TimestampIdFactory.get().newId();
        GetRequest req = new GetRequest(id, k);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new GetResponse(id, k, null, ResponseCode.CLIENT_TIMEOUT);
            }
            if (resp instanceof GetResponse) {
                return (GetResponse) resp;
            }
            return new GetResponse(id, k, null, ResponseCode.UNSUPPORTED_OP);
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new GetResponse(id, k, null, ResponseCode.CLIENT_TIMEOUT);
        }
    }

    public RangeResponse rangeRequest(KeyRange range) {
        return rangeRequest(range, Limit.noLimit());
    }
    
    public RangeResponse rangeRequest(KeyRange range, LimitTracker limit) {
        LOG.debug("RangeRequest for {}", range);
        long id = TimestampIdFactory.get().newId();
        RangeQuery.Request req = new RangeQuery.Request(id, range, limit, TFFactory.noTF(), RangeQuery.Type.SEQUENTIAL);
        worker.triggerOnSelf(req);
        try {
            CaracalResponse resp = responseQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new RangeResponse(id, range, ResponseCode.CLIENT_TIMEOUT, null, null);
            }
            if (resp instanceof RangeResponse) {
                return (RangeResponse) resp;
            }
            return new RangeResponse(id, range, ResponseCode.UNSUPPORTED_OP, null, null);
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new RangeResponse(id, range, ResponseCode.CLIENT_TIMEOUT, null, null);
        }
    }

    public DMMessage.ResponseCode putType(String putType) {
        LOG.debug("PutType for {}", putType);
        long id = TimestampIdFactory.get().newId();
        Gson gson = GsonHelper.getGson();
        TempTypeInfo typeInfo = gson.fromJson(putType, TempTypeInfo.class);
        PutType.Req req;
        try {
            req = new PutType.Req(id, typeInfo.dbId, typeInfo.typeId, putType.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
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
        long id = TimestampIdFactory.get().newId();
        Gson gson = GsonHelper.getGson();
        ClientGetTypeGson getTypeGson = gson.fromJson(getType, ClientGetTypeGson.class);
        GetType.Req req = new GetType.Req(id, getTypeGson.dbId, getTypeGson.typeId);
        worker.dataModelTrigger(req);
        try {
            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new GetType.Resp(id, DMMessage.ResponseCode.TIMEOUT, null);
            }
            if (resp instanceof GetType.Resp) {
                return (GetType.Resp) resp;
            }
            throw new RuntimeException("Bad Response Type");
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new GetType.Resp(id, DMMessage.ResponseCode.TIMEOUT, null);
        }
    }

    public DMMessage.ResponseCode putObj(String putObj) {
        LOG.debug("PutObj for {}", putObj);
        long id = TimestampIdFactory.get().newId();
        Gson gson = GsonHelper.getGson();
        ClientPutObjGson putObjGson = gson.fromJson(putObj, ClientPutObjGson.class);
        for (TempTypeInfo typeInfo : types.values()) {
            if (typeInfo.dbId.equals(putObjGson.dbId)) {
                TempObject.Value objValue = putObjGson.objValue.getValue(typeInfo);
                try {
                    PutObj.Req req = new PutObj.Req(id, putObjGson.dbId, putObjGson.typeId, putObjGson.objId, gson.toJson(objValue).getBytes("UTF-8"), getIndexValue(objValue, typeInfo));
                } catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                }
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
        }
        throw new RuntimeException("No Type"); //change to sent typeReq to caracal
    }

    private Map<ByteId, Object> getIndexValue(TempObject.Value objValue, TempTypeInfo typeInfo) {
        Map<ByteId, Object> indexValues = new TreeMap<ByteId, Object>();
        for (TempTypeInfo.TempFieldInfo fi : typeInfo.fieldMap.values()) {
            if (fi.indexed) {
                indexValues.put(fi.fieldId, objValue.fieldMap.get(fi.fieldId));
            }
        }
        return indexValues;
    }

    public GetObj.Resp getObj(String getObj) {
        LOG.debug("GetObj for {}", getObj);
        long id = TimestampIdFactory.get().newId();
        Gson gson = GsonHelper.getGson();
        ClientGetObjGson getObjGson = gson.fromJson(getObj, ClientGetObjGson.class);
        GetObj.Req req = new GetObj.Req(id, getObjGson.dbId, getObjGson.typeId, getObjGson.objId);
        worker.dataModelTrigger(req);
        try {
            DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
            if (resp == null) {
                return new GetObj.Resp(id, DMMessage.ResponseCode.TIMEOUT, getObjGson.dbId, getObjGson.typeId, getObjGson.objId, null);
            }
            if (resp instanceof GetObj.Resp) {
                return (GetObj.Resp) resp;
            }
            throw new RuntimeException("Bad Response Type");
        } catch (InterruptedException ex) {
            LOG.error("Couldn't get a response.", ex);
            return new GetObj.Resp(id, DMMessage.ResponseCode.TIMEOUT, getObjGson.dbId, getObjGson.typeId, getObjGson.objId, null);
        }
    }

    public QueryObj.Resp queryObj(String queryObj) {
        LOG.debug("QueryObj for {}", queryObj);
        long id = TimestampIdFactory.get().newId();
        Gson gson = GsonHelper.getGson();
        ClientQueryObjGson queryObjGson = gson.fromJson(queryObj, ClientQueryObjGson.class);
        for (TempTypeInfo typeInfo : types.values()) {
            if (typeInfo.dbId.equals(queryObjGson.typeId)) {
                Object indexVal = gson.fromJson(queryObjGson.indexValue, getClass(typeInfo.getField(queryObjGson.indexId).fieldType));
                QueryObj.Req req = new QueryObj.Req(id, queryObjGson.dbId, queryObjGson.typeId, queryObjGson.indexId, indexVal);
                worker.dataModelTrigger(req);
                try {
                    DMMessage.Resp resp = dataModelQueue.poll(TIMEOUT, TIMEUNIT);
                    if (resp == null) {
                        return new QueryObj.Resp(id, DMMessage.ResponseCode.TIMEOUT, null);
                    }
                    if (resp instanceof QueryObj.Resp) {
                        return (QueryObj.Resp) resp;
                    }
                    throw new RuntimeException("Bad Response Type");
                } catch (InterruptedException ex) {
                    LOG.error("Couldn't get a response.", ex);
                    return new QueryObj.Resp(id, DMMessage.ResponseCode.TIMEOUT, null);
                }
            }
        }
        throw new RuntimeException("No Type");
    }

    private Class<?> getClass(FieldInfo.FieldType fieldType) {
        if (fieldType == FieldInfo.FieldType.BOOLEAN) {
            return Boolean.class;
        } else if (fieldType == FieldInfo.FieldType.FLOAT) {
            return Float.class;
        } else if (fieldType == FieldInfo.FieldType.INTEGER) {
            return Integer.class;
        } else if (fieldType == FieldInfo.FieldType.STRING) {
            return String.class;
        } else {
            return null;
        }
    }
}
