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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.datamodel.DMCoreSerializer;
import se.sics.kompics.network.netty.serialization.Serializer;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil;
import se.sics.kompics.network.netty.serialization.SpecialSerializers.MessageSerializationUtil.MessageFields;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
public class DMNetMsgSerializer implements Serializer {

    private static final Logger LOG = LoggerFactory.getLogger(DMNetMsgSerializer.class);

    private final boolean REQ = false;
    private final boolean RESP = true;

    private final byte GETALLTYPES = (byte) 1;
    private final byte GETTYPE = (byte) 2;
    private final byte PUTTYPE = (byte) 3;
    private final byte GETOBJ = (byte) 4;
    private final byte PUTOBJ = (byte) 5;
    private final byte QUERYOBJ = (byte) 6;

    @Override
    public int identifier() {
        return DMCoreSerializer.DM.id;
    }

    @Override
    public void toBinary(Object o, ByteBuf buf) {
        if (o instanceof DMNetworkMessage.Req) {
            DMNetworkMessage.Req dmNetReq = (DMNetworkMessage.Req) o;
            MessageSerializationUtil.msgToBinary(dmNetReq, buf, false, REQ);
            toBinaryDMReq(dmNetReq.payload, buf);

        } else if (o instanceof DMNetworkMessage.Resp) {
            DMNetworkMessage.Resp dmNetResp = (DMNetworkMessage.Resp) o;
            MessageSerializationUtil.msgToBinary(dmNetResp, buf, false, RESP);
            toBinaryDMResp(dmNetResp.payload, buf);
        } else {
            LOG.warn("Couldn't serialize {}: {}", o, o.getClass());
        }
    }

    @Override
    public Object fromBinary(ByteBuf buf, Optional<Class> hint) {
        MessageFields fields = MessageSerializationUtil.msgFromBinary(buf);
        if (fields.flag2 == REQ) {
            DMMessage.Req req = fromBinaryDMReq(buf);
            return new DMNetworkMessage.Req(fields.src, fields.dst, req);
        } else if (fields.flag2 == RESP) {
            DMMessage.Resp resp = fromBinaryDMResp(buf);
            return new DMNetworkMessage.Resp(fields.src, fields.dst, resp);
        } else {
            LOG.warn("Couldn't deserialize - unknown flags {} {}", fields.flag1, fields.flag2);
            return null;
        }
    }

    private void toBinaryDMReq(DMMessage.Req req, ByteBuf buf) {
        try {
            if (req instanceof GetAllTypes.Req) {
                buf.writeByte(GETALLTYPES);
                DMMsgSerializer.serialize(GetAllTypes.Req.class, (GetAllTypes.Req) req, buf);
            } else if (req instanceof GetType.Req) {
                buf.writeByte(GETTYPE);
                DMMsgSerializer.serialize(GetType.Req.class, (GetType.Req) req, buf);
            } else if (req instanceof PutType.Req) {
                buf.writeByte(PUTTYPE);
                DMMsgSerializer.serialize(PutType.Req.class, (PutType.Req) req, buf);
            } else if (req instanceof GetObj.Req) {
                buf.writeByte(GETOBJ);
                DMMsgSerializer.serialize(GetObj.Req.class, (GetObj.Req) req, buf);
            } else if (req instanceof PutObj.Req) {
                buf.writeByte(PUTOBJ);
                DMMsgSerializer.serialize(PutObj.Req.class, (PutObj.Req) req, buf);
            } else if (req instanceof QueryObj.Req) {
                buf.writeByte(QUERYOBJ);
                DMMsgSerializer.serialize(QueryObj.Req.class, (QueryObj.Req) req, buf);
            } else {
                LOG.warn("Couldn't serialize {}: {}", req, req.getClass());
            }
        } catch (DMMsgSerializer.DMException ex) {
            LOG.warn("Couldn't serialize {}: {}", req, req.getClass());
        }
    }

    private void toBinaryDMResp(DMMessage.Resp resp, ByteBuf buf) {
        try {
            if (resp instanceof GetAllTypes.Resp) {
                buf.writeByte(GETALLTYPES);
                DMMsgSerializer.serialize(GetAllTypes.Resp.class, (GetAllTypes.Resp) resp, buf);
            } else if (resp instanceof GetType.Resp) {
                buf.writeByte(GETTYPE);
                DMMsgSerializer.serialize(GetType.Resp.class, (GetType.Resp) resp, buf);
            } else if (resp instanceof PutType.Resp) {
                buf.writeByte(PUTTYPE);
                DMMsgSerializer.serialize(PutType.Resp.class, (PutType.Resp) resp, buf);
            } else if (resp instanceof GetObj.Resp) {
                buf.writeByte(GETOBJ);
                DMMsgSerializer.serialize(GetObj.Resp.class, (GetObj.Resp) resp, buf);
            } else if (resp instanceof PutObj.Resp) {
                buf.writeByte(PUTOBJ);
                DMMsgSerializer.serialize(PutObj.Resp.class, (PutObj.Resp) resp, buf);
            } else if (resp instanceof QueryObj.Resp) {
                buf.writeByte(QUERYOBJ);
                DMMsgSerializer.serialize(QueryObj.Resp.class, (QueryObj.Resp) resp, buf);
            } else {
                LOG.warn("Couldn't serialize {}: {}", resp, resp.getClass());
            }
        } catch (DMMsgSerializer.DMException ex) {
            LOG.warn("Couldn't serialize {}: {}", resp, resp.getClass());
        }
    }

    private DMMessage.Req fromBinaryDMReq(ByteBuf buf) {
        try {
            byte type = buf.readByte();
            if (type == GETALLTYPES) {
                return DMMsgSerializer.deserialize(GetAllTypes.Req.class, buf);
            } else if (type == GETTYPE) {
                return DMMsgSerializer.deserialize(GetType.Req.class, buf);
            } else if (type == PUTTYPE) {
                return DMMsgSerializer.deserialize(PutType.Req.class, buf);
            } else if (type == GETOBJ) {
                return DMMsgSerializer.deserialize(GetObj.Req.class, buf);
            } else if (type == PUTOBJ) {
                return DMMsgSerializer.deserialize(PutObj.Req.class, buf);
            } else if (type == QUERYOBJ) {
                return DMMsgSerializer.deserialize(QueryObj.Req.class, buf);
            } else {
                LOG.warn("Couldn't deserialize req type {}", type);
                return null;
            }
        } catch (DMMsgSerializer.DMException ex) {
            LOG.warn("Couldn't deserialize - exception {}", ex);
            return null;
        }

    }

    private DMMessage.Resp fromBinaryDMResp(ByteBuf buf) {
        try {
            byte type = buf.readByte();
            if (type == GETALLTYPES) {
                return DMMsgSerializer.deserialize(GetAllTypes.Resp.class, buf);
            } else if (type == GETTYPE) {
                return DMMsgSerializer.deserialize(GetType.Resp.class, buf);
            } else if (type == PUTTYPE) {
                return DMMsgSerializer.deserialize(PutType.Resp.class, buf);
            } else if (type == GETOBJ) {
                return DMMsgSerializer.deserialize(GetObj.Resp.class, buf);
            } else if (type == PUTOBJ) {
                return DMMsgSerializer.deserialize(PutObj.Resp.class, buf);
            } else if (type == QUERYOBJ) {
                return DMMsgSerializer.deserialize(QueryObj.Resp.class, buf);
            } else {
                LOG.warn("Couldn't deserialize resp type {}", type);
                return null;
            }
        } catch (DMMsgSerializer.DMException ex) {
            LOG.warn("Couldn't deserialize - exception {}", ex);
            return null;
        }
    }
}