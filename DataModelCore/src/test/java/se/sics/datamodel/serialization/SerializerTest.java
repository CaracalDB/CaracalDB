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
package se.sics.datamodel.serialization;

import com.google.common.base.Optional;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import org.javatuples.Pair;
import org.javatuples.Triplet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import se.sics.caracaldb.store.Limit;
import se.sics.datamodel.DMCoreMessageRegistrator;
import se.sics.datamodel.QueryType;
import se.sics.datamodel.msg.DMMessage;
import se.sics.datamodel.msg.DMMsgSerializer;
import se.sics.datamodel.msg.DMNetworkMessage;
import se.sics.datamodel.msg.GetAllTypes;
import se.sics.datamodel.msg.GetObj;
import se.sics.datamodel.msg.GetType;
import se.sics.datamodel.msg.PutObj;
import se.sics.datamodel.msg.PutType;
import se.sics.datamodel.msg.QueryObj;
import se.sics.datamodel.util.ByteId;
import se.sics.datamodel.util.FieldIs;
import se.sics.datamodel.util.FieldScan;
import se.sics.kompics.address.Address;
import se.sics.kompics.network.netty.serialization.Serializers;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */

@RunWith(JUnit4.class)
public class SerializerTest {
    @Test
    public void testResponseCode() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        DMMessage.ResponseCode respCode;
        
        buf = Unpooled.buffer();
        DMMsgSerializer.serialize(DMMessage.ResponseCode.class, DMMessage.ResponseCode.SUCCESS, buf);
        respCode = DMMsgSerializer.deserialize(DMMessage.ResponseCode.class, buf);
        Assert.assertEquals(DMMessage.ResponseCode.SUCCESS, respCode);
        buf.release();
        
        buf = Unpooled.buffer();
        DMMsgSerializer.serialize(DMMessage.ResponseCode.class, DMMessage.ResponseCode.FAILURE, buf);
        respCode = DMMsgSerializer.deserialize(DMMessage.ResponseCode.class, buf);
        Assert.assertEquals(DMMessage.ResponseCode.FAILURE, respCode);
        buf.release();
        
        buf = Unpooled.buffer();
        DMMsgSerializer.serialize(DMMessage.ResponseCode.class, DMMessage.ResponseCode.TIMEOUT, buf);
        respCode = DMMsgSerializer.deserialize(DMMessage.ResponseCode.class, buf);
        Assert.assertEquals(DMMessage.ResponseCode.TIMEOUT, respCode);
        buf.release();
    }
    
    @Test
    public void testGetAllTypesReq() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        GetAllTypes.Req req, desReq;
        
        buf = Unpooled.buffer();
        req = new GetAllTypes.Req(1, new ByteId(new byte[]{1,1}));
        DMMsgSerializer.serialize(GetAllTypes.Req.class, req, buf);
        desReq = DMMsgSerializer.deserialize(GetAllTypes.Req.class, buf);
        Assert.assertEquals(req, desReq);
        buf.release();
    }
    
    @Test
    public void testGetAllTypesResp() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        GetAllTypes.Resp resp, desResp;
        Map<String, ByteId> types;
        
        buf = Unpooled.buffer();
        types = new TreeMap<>();
        resp = new GetAllTypes.Resp(1, DMMessage.ResponseCode.SUCCESS, new ByteId(new byte[]{1,1}), types);
        DMMsgSerializer.serialize(GetAllTypes.Resp.class, resp, buf);
        desResp = DMMsgSerializer.deserialize(GetAllTypes.Resp.class, buf);
        Assert.assertEquals(resp, desResp);
        buf.release();
        
        buf = Unpooled.buffer();
        types = new TreeMap<>();
        types.put("type1", new ByteId(new byte[]{1,2}));
        resp = new GetAllTypes.Resp(1, DMMessage.ResponseCode.SUCCESS, new ByteId(new byte[]{1,1}), types);
        DMMsgSerializer.serialize(GetAllTypes.Resp.class, resp, buf);
        desResp = DMMsgSerializer.deserialize(GetAllTypes.Resp.class, buf);
        Assert.assertEquals(resp, desResp);
        buf.release();
    }
    
    @Test
    public void testGetTypeReq() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        GetType.Req req, desReq;
        
        buf = Unpooled.buffer();
        req = new GetType.Req(1, Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2})));
        DMMsgSerializer.serialize(GetType.Req.class, req, buf);
        desReq = DMMsgSerializer.deserialize(GetType.Req.class, buf);
        Assert.assertEquals(req, desReq);
        buf.release();
    }
    
    @Test
    public void testGetTypeResp() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        GetType.Resp resp, desResp;
        
        buf = Unpooled.buffer();
        resp = new GetType.Resp(1, DMMessage.ResponseCode.SUCCESS, Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2})), new byte[]{1,1});
        DMMsgSerializer.serialize(GetType.Resp.class, resp, buf);
        desResp = DMMsgSerializer.deserialize(GetType.Resp.class, buf);
        Assert.assertEquals(resp, desResp);
        buf.release();
    }

    @Test
    public void testPutTypeReq() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        PutType.Req req, desReq;
        
        buf = Unpooled.buffer();
        req = new PutType.Req(1, Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2})), new byte[]{1,1});
        DMMsgSerializer.serialize(PutType.Req.class, req, buf);
        desReq = DMMsgSerializer.deserialize(PutType.Req.class, buf);
        Assert.assertEquals(req, desReq);
        buf.release();
    }
    
    @Test
    public void testPutTypeResp() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        PutType.Resp resp, desResp;
        
        buf = Unpooled.buffer();
        resp = new PutType.Resp(1, DMMessage.ResponseCode.SUCCESS, Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2})));
        DMMsgSerializer.serialize(PutType.Resp.class, resp, buf);
        desResp = DMMsgSerializer.deserialize(PutType.Resp.class, buf);
        Assert.assertEquals(resp, desResp);
        buf.release();
    }
    
    @Test
    public void testGetObjReq() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        GetObj.Req req, desReq;
        
        buf = Unpooled.buffer();
        req = new GetObj.Req(1, Triplet.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2}), new ByteId(new byte[]{1,3})));
        DMMsgSerializer.serialize(GetObj.Req.class, req, buf);
        desReq = DMMsgSerializer.deserialize(GetObj.Req.class, buf);
        Assert.assertEquals(req, desReq);
        buf.release();
    }
    
    @Test
    public void testGetObjResp() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        GetObj.Resp resp, desResp;
        
        buf = Unpooled.buffer();
        resp = new GetObj.Resp(1, DMMessage.ResponseCode.SUCCESS, Triplet.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2}), new ByteId(new byte[]{1,3})), new byte[]{1,1});
        DMMsgSerializer.serialize(GetObj.Resp.class, resp, buf);
        desResp = DMMsgSerializer.deserialize(GetObj.Resp.class, buf);
        Assert.assertEquals(resp, desResp);
        buf.release();
    }
    
    @Test
    public void testPutObjReq() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        PutObj.Req req, desReq;
        Map<ByteId, Object> indexes;
        
        buf = Unpooled.buffer();
        indexes = new TreeMap<>();
        req = new PutObj.Req(1, Triplet.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2}), new ByteId(new byte[]{1,3})), new byte[]{1,1}, indexes);
        DMMsgSerializer.serialize(PutObj.Req.class, req, buf);
        desReq = DMMsgSerializer.deserialize(PutObj.Req.class, buf);
        Assert.assertEquals(req, desReq);
        buf.release();
        
        buf = Unpooled.buffer();
        indexes = new TreeMap<>();
        long l = 15;
        indexes.put(new ByteId(new byte[]{1,1}), (Integer)1);
        indexes.put(new ByteId(new byte[]{1,2}), (Long)l);
        indexes.put(new ByteId(new byte[]{1,3}), (Boolean)true);
        indexes.put(new ByteId(new byte[]{1,4}), "something");
        req = new PutObj.Req(1, Triplet.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2}), new ByteId(new byte[]{1,3})), new byte[]{1,1}, indexes);
        DMMsgSerializer.serialize(PutObj.Req.class, req, buf);
        desReq = DMMsgSerializer.deserialize(PutObj.Req.class, buf);
        Assert.assertEquals(req, desReq);
        buf.release();
    }
    
    @Test
    public void testPutObjResp() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        PutObj.Resp resp, desResp;
        
        buf = Unpooled.buffer();
        resp = new PutObj.Resp(1, DMMessage.ResponseCode.SUCCESS, Triplet.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2}), new ByteId(new byte[]{1,3})));
        DMMsgSerializer.serialize(PutObj.Resp.class, resp, buf);
        desResp = DMMsgSerializer.deserialize(PutObj.Resp.class, buf);
        Assert.assertEquals(resp, desResp);
        buf.release();
    }
    
    @Test
    public void testQueryObjReq() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        QueryObj.Req req, desReq;
        QueryType queryType;
        
        buf = Unpooled.buffer();
        queryType = new FieldIs(1);
        req = new QueryObj.Req(1, Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2})), new ByteId(new byte[]{1,3}), queryType, Limit.noLimit());
        DMMsgSerializer.serialize(QueryObj.Req.class, req, buf);
        desReq = DMMsgSerializer.deserialize(QueryObj.Req.class, buf);
        Assert.assertEquals(req, desReq);
        buf.release();
        
        buf = Unpooled.buffer();
        queryType = new FieldScan(1, 3);
        req = new QueryObj.Req(1, Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2})), new ByteId(new byte[]{1,3}), queryType, Limit.noLimit());
        DMMsgSerializer.serialize(QueryObj.Req.class, req, buf);
        desReq = DMMsgSerializer.deserialize(QueryObj.Req.class, buf);
        Assert.assertEquals(req, desReq);
        buf.release();
    }
    
    @Test
    public void testQueryObjResp() throws DMMsgSerializer.DMException {
        ByteBuf buf;
        QueryObj.Resp resp, desResp;
        Map<ByteId, ByteBuffer> objs;
        
        buf = Unpooled.buffer();
        objs = new TreeMap<>();
        resp = new QueryObj.Resp(1, DMMessage.ResponseCode.SUCCESS, Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2})), objs);
        DMMsgSerializer.serialize(QueryObj.Resp.class, resp, buf);
        desResp = DMMsgSerializer.deserialize(QueryObj.Resp.class, buf);
        Assert.assertEquals(resp, desResp);
        buf.release();
        
        buf = Unpooled.buffer();
        objs = new TreeMap<>();
        objs.put(new ByteId(new byte[]{1,1}), ByteBuffer.wrap(new byte[]{1,1}));
        resp = new QueryObj.Resp(1, DMMessage.ResponseCode.SUCCESS, Pair.with(new ByteId(new byte[]{1,1}), new ByteId(new byte[]{1,2})), objs);
        DMMsgSerializer.serialize(QueryObj.Resp.class, resp, buf);
        desResp = DMMsgSerializer.deserialize(QueryObj.Resp.class, buf);
        Assert.assertEquals(resp, desResp);
        buf.release();
    }
    
    @Test 
    public void testNetMsg() throws UnknownHostException {
        DMCoreMessageRegistrator.register();
        
        Address self = new Address(InetAddress.getLocalHost(), 12345, new byte[]{1,2,3});
        ByteBuf buf;
        DMNetworkMessage.Req req, desReq;
        DMNetworkMessage.Resp resp, desResp;
        
        buf = Unpooled.buffer();
        req = new DMNetworkMessage.Req(self, self, new GetAllTypes.Req(1, new ByteId(new byte[]{1,1})));
        Serializers.toBinary(req, buf);
        desReq = (DMNetworkMessage.Req)Serializers.fromBinary(buf, Optional.absent());
        Assert.assertEquals(req, desReq);
        buf.release();
        
        buf = Unpooled.buffer();
        Map<String, ByteId> typesMap = new TreeMap<>();
        resp = new DMNetworkMessage.Resp(self, self, new GetAllTypes.Resp(1, DMMessage.ResponseCode.SUCCESS, new ByteId(new byte[]{1,1}), typesMap));
        Serializers.toBinary(resp, buf);
        desResp = (DMNetworkMessage.Resp)Serializers.fromBinary(buf, Optional.absent());
        Assert.assertEquals(resp, desResp);
        buf.release();
    }
}