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
package se.sics.datamodel.msg;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.UUID;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import se.sics.caracaldb.store.Limit.LimitTracker;
import se.sics.datamodel.QueryType;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.util.FieldIs;
import se.sics.datamodel.util.FieldScan;
import se.sics.kompics.network.netty.serialization.Serializers;
import se.sics.kompics.network.netty.serialization.SpecialSerializers;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMMsgSerializer {

    private static final Map<Class<?>, DMSerializer<?>> instance;

    static {
        instance = new HashMap<Class<?>, DMSerializer<?>>();
        instance.put(ByteId.class, new ByteIdSerializer());
        instance.put(DMMessage.ResponseCode.class, new ResponseCodeSerializer());
        instance.put(GetAllTypes.Req.class, new GetAllTypesReqSerializer());
        instance.put(GetAllTypes.Resp.class, new GetAllTypesRespSerializer());
        instance.put(GetType.Req.class, new GetTypeReqSerializer());
        instance.put(GetType.Resp.class, new GetTypeRespSerializer());
        instance.put(PutType.Req.class, new PutTypeReqSerializer());
        instance.put(PutType.Resp.class, new PutTypeRespSerializer());
        instance.put(GetObj.Req.class, new GetObjReqSerializer());
        instance.put(GetObj.Resp.class, new GetObjRespSerializer());
        instance.put(PutObj.Req.class, new PutObjReqSerializer());
        instance.put(PutObj.Resp.class, new PutObjRespSerializer());
        instance.put(QueryObj.Req.class, new QueryObjReqSerializer());
        instance.put(QueryObj.Resp.class, new QueryObjRespSerializer());
    }

    public static <T> void serialize(Class<T> objClass, T obj, ByteBuf buf) throws DMException {
        DMSerializer<T> s = (DMSerializer<T>) instance.get(objClass);
        if (s == null) {
            throw new DMException("no serializer for class " + objClass);
        }
        s.serialize(obj, buf);
    }

    public static <T> T deserialize(Class<T> objClass, ByteBuf buf) throws DMException {
        DMSerializer<T> s = (DMSerializer<T>) instance.get(objClass);
        if (s == null) {
            throw new DMException("no serializer for class " + objClass);
        }
        return s.deserialize(buf);
    }

    protected static interface DMSerializer<T> {

        void serialize(T obj, ByteBuf buf) throws DMException;

        T deserialize(ByteBuf bytes) throws DMException;
    }

    public static class DMException extends Exception {

        public DMException(Throwable cause) {
            super(cause);
        }

        public DMException(String msg) {
            super(msg);
        }
    }

    private static class GetAllTypesReqSerializer implements DMSerializer<GetAllTypes.Req> {

        @Override
        public void serialize(GetAllTypes.Req req, ByteBuf buf) {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(req.id, buf);
            buf.writeInt(req.dbId.getId().length);
            buf.writeBytes(req.dbId.getId());
        }

        @Override
        public GetAllTypes.Req deserialize(ByteBuf buf) {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            int dbIdLength = buf.readInt();
            byte[] dbIdByte = new byte[dbIdLength];
            buf.readBytes(dbIdByte);
            ByteId dbId = new ByteId(dbIdByte);
            return new GetAllTypes.Req(id, dbId);
        }

    }

    private static class GetAllTypesRespSerializer implements DMSerializer<GetAllTypes.Resp> {

        @Override
        public void serialize(GetAllTypes.Resp resp, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(resp.id, buf);
            DMMsgSerializer.serialize(ByteId.class, resp.dbId, buf);
            DMMsgSerializer.serialize(DMMessage.ResponseCode.class, resp.respCode, buf);
            serializeTypes(resp.types, buf);
        }

        @Override
        public GetAllTypes.Resp deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            DMMessage.ResponseCode respCode = DMMsgSerializer.deserialize(DMMessage.ResponseCode.class, buf);
            Map<String, ByteId> types = deserializeTypes(buf);
            return new GetAllTypes.Resp(id, respCode, dbId, types);
        }

        private void serializeTypes(Map<String, ByteId> types, ByteBuf buf) throws DMException {
            buf.writeInt(types.size());
            for (Entry<String, ByteId> type : types.entrySet()) {
                try {
                    byte[] keyBytes = type.getKey().getBytes("UTF8");
                    buf.writeInt(keyBytes.length);
                    buf.writeBytes(keyBytes);
                    DMMsgSerializer.serialize(ByteId.class, type.getValue(), buf);
                } catch (UnsupportedEncodingException ex) {
                    throw new DMException(ex);
                }

            }
        }

        private Map<String, ByteId> deserializeTypes(ByteBuf buf) throws DMException {
            Map<String, ByteId> result = new TreeMap<String, ByteId>();
            int typesLength = buf.readInt();
            for (; typesLength > 0; typesLength--) {
                try {
                    int keyLength = buf.readInt();
                    byte[] keyBytes = new byte[keyLength];
                    buf.readBytes(keyBytes);
                    String key = new String(keyBytes, "UTF8");

                    ByteId val = DMMsgSerializer.deserialize(ByteId.class, buf);
                    result.put(key, val);
                } catch (UnsupportedEncodingException ex) {
                    throw new RuntimeException(ex);
                }
            }
            return result;
        }
    }

    private static class GetTypeReqSerializer implements DMSerializer<GetType.Req> {

        @Override
        public void serialize(GetType.Req req, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(req.id, buf);
            DMMsgSerializer.serialize(ByteId.class, req.typeId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, req.typeId.getValue1(), buf);
        }

        @Override
        public GetType.Req deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            return new GetType.Req(id, Pair.with(dbId, typeId));
        }
    }

    private static class GetTypeRespSerializer implements DMSerializer<GetType.Resp> {

        @Override
        public void serialize(GetType.Resp resp, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(resp.id, buf);
            DMMsgSerializer.serialize(DMMessage.ResponseCode.class, resp.respCode, buf);
            DMMsgSerializer.serialize(ByteId.class, resp.typeId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, resp.typeId.getValue1(), buf);
            if (resp.typeInfo == null) {
                buf.writeInt(0);
            } else {
                buf.writeInt(resp.typeInfo.length);
                buf.writeBytes(resp.typeInfo);
            }
        }

        @Override
        public GetType.Resp deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            DMMessage.ResponseCode respCode = DMMsgSerializer.deserialize(DMMessage.ResponseCode.class, buf);
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            int payloadLength = buf.readInt();
            byte[] payload;
            if (payloadLength == 0) {
                payload = null;
            } else {
                payload = new byte[payloadLength];
                buf.readBytes(payload);
            }
            return new GetType.Resp(id, respCode, Pair.with(dbId, typeId), payload);
        }
    }
    
    private static class PutTypeReqSerializer implements DMSerializer<PutType.Req> {

        @Override
        public void serialize(PutType.Req req, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(req.id, buf);
            DMMsgSerializer.serialize(ByteId.class, req.typeId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, req.typeId.getValue1(), buf);
            buf.writeInt(req.typeInfo.length);
            buf.writeBytes(req.typeInfo);
        }

        @Override
        public PutType.Req deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            int typeInfoLength = buf.readInt();
            byte[] typeInfo = new byte[typeInfoLength];
            buf.readBytes(typeInfo);
            return new PutType.Req(id, Pair.with(dbId, typeId), typeInfo);
        }
    }

    private static class PutTypeRespSerializer implements DMSerializer<PutType.Resp> {

        @Override
        public void serialize(PutType.Resp resp, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(resp.id, buf);
            DMMsgSerializer.serialize(DMMessage.ResponseCode.class, resp.respCode, buf);
            DMMsgSerializer.serialize(ByteId.class, resp.typeId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, resp.typeId.getValue1(), buf);
        }

        @Override
        public PutType.Resp deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            DMMessage.ResponseCode respCode = DMMsgSerializer.deserialize(DMMessage.ResponseCode.class, buf);
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            return new PutType.Resp(id, respCode, Pair.with(dbId, typeId));
        }
    }
    private static class GetObjReqSerializer implements DMSerializer<GetObj.Req> {

        @Override
        public void serialize(GetObj.Req req, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(req.id, buf);
            DMMsgSerializer.serialize(ByteId.class, req.objId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, req.objId.getValue1(), buf);
            DMMsgSerializer.serialize(ByteId.class, req.objId.getValue2(), buf);
        }

        @Override
        public GetObj.Req deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId objId = DMMsgSerializer.deserialize(ByteId.class, buf);

            return new GetObj.Req(id, Triplet.with(dbId, typeId, objId));
        }
    }

    private static class GetObjRespSerializer implements DMSerializer<GetObj.Resp> {

        @Override
        public void serialize(GetObj.Resp resp, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(resp.id, buf);
            DMMsgSerializer.serialize(DMMessage.ResponseCode.class, resp.respCode, buf);
            DMMsgSerializer.serialize(ByteId.class, resp.objId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, resp.objId.getValue1(), buf);
            DMMsgSerializer.serialize(ByteId.class, resp.objId.getValue2(), buf);
            buf.writeInt(resp.value.length);
            buf.writeBytes(resp.value);
        }

        @Override
        public GetObj.Resp deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            DMMessage.ResponseCode respCode = DMMsgSerializer.deserialize(DMMessage.ResponseCode.class, buf);
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId objId = DMMsgSerializer.deserialize(ByteId.class, buf);
            int payloadLength = buf.readInt();
            byte[] payload = new byte[payloadLength];
            buf.readBytes(payload);
            return new GetObj.Resp(id, respCode, Triplet.with(dbId, typeId, objId), payload);
        }
    }

    private static class PutObjReqSerializer implements DMSerializer<PutObj.Req> {

        @Override
        public void serialize(PutObj.Req req, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(req.id, buf);
            DMMsgSerializer.serialize(ByteId.class, req.objId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, req.objId.getValue1(), buf);
            DMMsgSerializer.serialize(ByteId.class, req.objId.getValue2(), buf);
            buf.writeInt(req.objValue.length);
            buf.writeBytes(req.objValue);
            serializeIndexes(req.indexValue, buf);
        }

        @Override
        public PutObj.Req deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId objId = DMMsgSerializer.deserialize(ByteId.class, buf);
            int objValueLength = buf.readInt();
            byte[] objValue = new byte[objValueLength];
            buf.readBytes(objValue);
            Map<ByteId, Object> indexes = deserializeIndexes(buf);
            return new PutObj.Req(id, Triplet.with(dbId, typeId, objId), objValue, indexes);
        }

        private void serializeIndexes(Map<ByteId, Object> indexes, ByteBuf buf) throws DMException {
            buf.writeInt(indexes.size());
            for (Entry<ByteId, Object> index : indexes.entrySet()) {
                DMMsgSerializer.serialize(ByteId.class, index.getKey(), buf);
                Helper.serializeIndex(index.getValue(), buf);
            }
        }

        private Map<ByteId, Object> deserializeIndexes(ByteBuf buf) throws DMException {
            Map<ByteId, Object> result = new TreeMap<ByteId, Object>();
            int indexesLength = buf.readInt();
            for (; indexesLength > 0; indexesLength--) {
                ByteId key = DMMsgSerializer.deserialize(ByteId.class, buf);
                Object val = Helper.deserializeIndex(buf);
                result.put(key, val);
            }
            return result;
        }
    }

    private static class PutObjRespSerializer implements DMSerializer<PutObj.Resp> {

        @Override
        public void serialize(PutObj.Resp resp, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(resp.id, buf);
            DMMsgSerializer.serialize(DMMessage.ResponseCode.class, resp.respCode, buf);
            DMMsgSerializer.serialize(ByteId.class, resp.objId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, resp.objId.getValue1(), buf);
            DMMsgSerializer.serialize(ByteId.class, resp.objId.getValue2(), buf);
        }

        @Override
        public PutObj.Resp deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            DMMessage.ResponseCode respCode = DMMsgSerializer.deserialize(DMMessage.ResponseCode.class, buf);
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId objId = DMMsgSerializer.deserialize(ByteId.class, buf);
            return new PutObj.Resp(id, respCode, Triplet.with(dbId, typeId, objId));
        }
    }

    private static class QueryObjReqSerializer implements DMSerializer<QueryObj.Req> {

        @Override
        public void serialize(QueryObj.Req req, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(req.id, buf);
            DMMsgSerializer.serialize(ByteId.class, req.typeId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, req.typeId.getValue1(), buf);
            DMMsgSerializer.serialize(ByteId.class, req.indexId, buf);
            if (req.indexVal instanceof FieldIs) {
                FieldIs queryType = (FieldIs) req.indexVal;
                buf.writeByte(1);
                Helper.serializeIndex(queryType.indexVal, buf);
            } else if (req.indexVal instanceof FieldScan) {
                FieldScan queryType = (FieldScan) req.indexVal;
                buf.writeByte(2);
                Helper.serializeIndex(queryType.from, buf);
                Helper.serializeIndex(queryType.to, buf);
            } else {
                throw new DMException("cannot serialize queryType");
            }

            //TODO later - remove the Serializers dependency
            Serializers.toBinary(req.limit, buf);
        }

        @Override
        public QueryObj.Req deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId indexId = DMMsgSerializer.deserialize(ByteId.class, buf);
            byte type = buf.readByte();
            QueryType queryType;
            switch (type) {
                case 1:
                    Object indexVal = Helper.deserializeIndex(buf);
                    queryType = new FieldIs(indexVal);
                    break;
                case 2:
                    Object from = Helper.deserializeIndex(buf);
                    Object to = Helper.deserializeIndex(buf);
                    queryType = new FieldScan(from, to);
                    break;
                default:
                    throw new DMException("cannot deserialize queryType");
            }
            LimitTracker limit = (LimitTracker) Serializers.fromBinary(buf, Optional.absent());

            return new QueryObj.Req(id, Pair.with(dbId, typeId), indexId, queryType, limit);
        }

    }

    private static class QueryObjRespSerializer implements DMSerializer<QueryObj.Resp> {

        @Override
        public void serialize(QueryObj.Resp resp, ByteBuf buf) throws DMException {
            SpecialSerializers.UUIDSerializer.INSTANCE.toBinary(resp.id, buf);
            DMMsgSerializer.serialize(DMMessage.ResponseCode.class, resp.respCode, buf);
            DMMsgSerializer.serialize(ByteId.class, resp.typeId.getValue0(), buf);
            DMMsgSerializer.serialize(ByteId.class, resp.typeId.getValue1(), buf);
            serializeObjects(resp.objs, buf);
        }

        @Override
        public QueryObj.Resp deserialize(ByteBuf buf) throws DMException {
            UUID id = (UUID) SpecialSerializers.UUIDSerializer.INSTANCE.fromBinary(buf, Optional.absent());
            DMMessage.ResponseCode respCode = DMMsgSerializer.deserialize(DMMessage.ResponseCode.class, buf);
            ByteId dbId = DMMsgSerializer.deserialize(ByteId.class, buf);
            ByteId typeId = DMMsgSerializer.deserialize(ByteId.class, buf);
            Map<ByteId, ByteBuffer> objs = deserializeObjects(buf);
            return new QueryObj.Resp(id, respCode, Pair.with(dbId, typeId), objs);
        }

        private void serializeObjects(Map<ByteId, ByteBuffer> objects, ByteBuf buf) throws DMException {
            buf.writeInt(objects.size());
            for (Entry<ByteId, ByteBuffer> obj : objects.entrySet()) {
                DMMsgSerializer.serialize(ByteId.class, obj.getKey(), buf);
                buf.writeInt(obj.getValue().remaining());
                buf.writeBytes(obj.getValue().array());
            }
        }

        private Map<ByteId, ByteBuffer> deserializeObjects(ByteBuf buf) throws DMException {
            Map<ByteId, ByteBuffer> result = new TreeMap<ByteId, ByteBuffer>();
            int objectsLength = buf.readInt();
            for (; objectsLength > 0; objectsLength--) {
                ByteId key = DMMsgSerializer.deserialize(ByteId.class, buf);
                int objectLength = buf.readInt();
                byte[] val = new byte[objectLength];
                buf.readBytes(val);
                result.put(key, ByteBuffer.wrap(val));
            }
            return result;
        }
    }

    private static class ResponseCodeSerializer implements DMSerializer<DMMessage.ResponseCode> {

        @Override
        public void serialize(DMMessage.ResponseCode respCode, ByteBuf buf) throws DMException {
            switch (respCode) {
                case SUCCESS:
                    buf.writeByte(0);
                    return;
                case FAILURE:
                    buf.writeByte(1);
                    return;
                case TIMEOUT:
                    buf.writeByte(2);
                    return;
                default:
                    throw new DMException("unknown response code " + respCode);
            }
        }

        @Override
        public DMMessage.ResponseCode deserialize(ByteBuf bytes) throws DMException {
            byte respCode = bytes.readByte();
            switch (respCode) {
                case 0:
                    return DMMessage.ResponseCode.SUCCESS;
                case 1:
                    return DMMessage.ResponseCode.FAILURE;
                case 2:
                    return DMMessage.ResponseCode.TIMEOUT;
                default:
                    throw new DMException("unknown response code " + respCode);
            }
        }
    }

    private static class ByteIdSerializer implements DMSerializer<ByteId> {

        @Override
        public void serialize(ByteId byteId, ByteBuf buf) throws DMException {
            buf.writeInt(byteId.getId().length);
            buf.writeBytes(byteId.getId());
        }

        @Override
        public ByteId deserialize(ByteBuf buf) throws DMException {
            int byteIdLength = buf.readInt();
            byte[] byteIdBytes = new byte[byteIdLength];
            buf.readBytes(byteIdBytes);
            return new ByteId(byteIdBytes);
        }
    }

    private static class Helper {

        public static void serializeIndex(Object indexVal, ByteBuf buf) throws DMException {
            if (indexVal instanceof Boolean) {
                buf.writeByte(1);
                buf.writeBoolean((Boolean) indexVal);
                return;
            } else if (indexVal instanceof Integer) {
                buf.writeByte(2);
                buf.writeInt((Integer) indexVal);
                return;
            } else if (indexVal instanceof Long) {
                buf.writeByte(3);
                buf.writeLong((Long) indexVal);
                return;
            } else if (indexVal instanceof String) {
                try {
                    buf.writeByte(4);
                    byte[] stringBytes = ((String) indexVal).getBytes("UTF8");
                    buf.writeInt(stringBytes.length);
                    buf.writeBytes(stringBytes);
                    return;
                } catch (UnsupportedEncodingException ex) {
                    throw new DMException(ex);
                }
            }
            throw new DMException("cannot serialize indexVal");
        }

        public static Object deserializeIndex(ByteBuf buf) throws DMException {
            byte type = buf.readByte();
            switch (type) {
                case 1:
                    return buf.readBoolean();
                case 2:
                    return buf.readInt();
                case 3:
                    return buf.readLong();
                case 4:
                    try {
                        int stringLength = buf.readInt();
                        byte[] stringBytes = new byte[stringLength];
                        buf.readBytes(stringBytes);
                        return new String(stringBytes, "UTF8");
                    } catch (UnsupportedEncodingException ex) {
                        throw new DMException(ex);
                    }
                default:
                    throw new DMException("cannot deserialize indexVal");
            }
        }
    }
}
