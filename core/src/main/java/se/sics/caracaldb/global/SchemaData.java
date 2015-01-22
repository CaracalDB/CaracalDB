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
package se.sics.caracaldb.global;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Holder class for Schema information.
 *<p>
 * Observed MetaData keys: <br>
 * algo -> {paxos} (default: paxos)<br>
 * db -> {memory, leveldb} (default: leveldb) you can also register others in the server's config<br>
 * vnodes -> {1, ..., n} (default: 1)<br>
 * rfactor -> {1, ..., n} (default: 3) preferably odd<br>
 * <p>
 * DON'T use the following unless you know what you are doing!<br>
 * id -> {e.g. "0F"} (default: auto-generated) this forces the schema to be assigned the given HEX-id.<br>
 * forceMaster -> {true, false} (default: false) this forces the schema to be assigned replication group 0 (the master group)<br>
 *
 * @author lkroll
 */
public class SchemaData {

    public static final Charset CHARSET = Charset.forName("UTF-8");

    long version;
    final Map<String, ByteBuffer> schemaIDs = new HashMap<String, ByteBuffer>();
    final Map<ByteBuffer, String> schemaNames = new TreeMap<ByteBuffer, String>();
    final Map<ByteBuffer, ImmutableMap<String, String>> metaData = new TreeMap<ByteBuffer, ImmutableMap<String, String>>();

    public byte[] getId(String name) {
        ByteBuffer buf = schemaIDs.get(name);
        if (buf != null) {
            return buf.array();
        }
        return null;
    }

    public String getName(byte[] id) {
        return schemaNames.get(ByteBuffer.wrap(id));
    }

    public String getMetaValue(byte[] id, String key) {
        ImmutableMap<String, String> meta = metaData.get(ByteBuffer.wrap(id));
        if (meta != null) {
            return meta.get(key);
        }
        return null;
    }
    
    public ImmutableSortedSet<String> schemas() {
        return ImmutableSortedSet.copyOf(schemaIDs.keySet());
    }
    
    public String schemaInfo(String name) {
        ByteBuffer id = schemaIDs.get(name);
        if (id == null) {
            return "Not found!";
        }
        ImmutableMap<String, String> meta = metaData.get(id);
        if (meta == null) {
            return "No info!";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(" ( \n");
        for (Entry<String, String> e : meta.entrySet()) {
            sb.append("   ");
            sb.append(e.getKey());
            sb.append(" -> ");
            sb.append(e.getValue());
            sb.append("\n");
        }
        sb.append(" )");
        return sb.toString();
    }

    public byte[] serialise() {
        ByteBuf buf = Unpooled.buffer();

        serialise(buf);

        byte[] data = new byte[buf.readableBytes()];
        buf.readBytes(data);
        buf.release();

        return data;
    }
    
    public void serialise(ByteBuf buf) {
        buf.writeLong(version);
        serialiseData(buf);
    }

    public static SchemaData deserialise(byte[] bytes) {

        ByteBuf buf = Unpooled.wrappedBuffer(bytes);

        return deserialise(buf);
    }
    
    public static SchemaData deserialise(ByteBuf buf) {
        SchemaData sd = new SchemaData();
        sd.version = buf.readLong();

        deserialiseData(buf, sd);

        return sd;
    }

    private void serialiseData(ByteBuf buf) {
        buf.writeInt(schemaNames.size());
        for (Entry<ByteBuffer, String> e : schemaNames.entrySet()) {
            ByteBuffer id = e.getKey();
            String name = e.getValue();
            ImmutableMap<String, String> meta = metaData.get(id);
            serialiseSchema(buf, id, name, meta);
        }
    }

    private static void deserialiseData(ByteBuf buf, SchemaData sd) {
        int schemaNum = buf.readInt();
        for (int i = 0; i < schemaNum; i++) {
            SingleSchema schema = deserialiseSchema(buf);
            sd.schemaIDs.put(schema.name, schema.id);
            sd.schemaNames.put(schema.id, schema.name);
            sd.metaData.put(schema.id, schema.meta);
        }
    }

    public static void serialiseSchema(ByteBuf buf, SingleSchema schema) {
        serialiseSchema(buf, schema.id, schema.name, schema.meta);
    }
    
    static void serialiseSchema(ByteBuf buf, ByteBuffer id, String name, ImmutableMap<String, String> metaData) {
        buf.writeInt(id.array().length);
        buf.writeBytes(id.array());
        byte[] nameB = name.getBytes(CHARSET);
        buf.writeInt(nameB.length);
        buf.writeBytes(nameB);
        buf.writeInt(metaData.size());
        for (Entry<String, String> e : metaData.entrySet()) {
            byte[] keyB = e.getKey().getBytes(CHARSET);
            byte[] valB = e.getValue().getBytes(CHARSET);
            buf.writeInt(keyB.length);
            buf.writeBytes(keyB);
            buf.writeInt(valB.length);
            buf.writeBytes(valB);
        }
    }

    public static SingleSchema deserialiseSchema(ByteBuf buf) {
        int idL = buf.readInt();
        byte[] id = new byte[idL];
        buf.readBytes(id);
        ByteBuffer idW = ByteBuffer.wrap(id);
        int nameL = buf.readInt();
        byte[] nameB = new byte[nameL];
        buf.readBytes(nameB);
        String name = new String(nameB, CHARSET);
        ImmutableMap.Builder<String, String> meta = ImmutableMap.builder();
        int metaNum = buf.readInt();
        for (int j = 0; j < metaNum; j++) {
            int keyL = buf.readInt();
            byte[] keyB = new byte[keyL];
            buf.readBytes(keyB);
            String key = new String(keyB, CHARSET);
            int valL = buf.readInt();
            byte[] valB = new byte[valL];
            buf.readBytes(valB);
            String val = new String(valB, CHARSET);
            meta.put(key, val);
        }
        return new SingleSchema(idW, name, meta.build());
    }
    
    public static class SingleSchema {
        public final ByteBuffer id;
        public final String name;
        public final ImmutableMap<String, String> meta;
        
        public SingleSchema(ByteBuffer id, String name, ImmutableMap<String, String> meta) {
            this.id = id;
            this.name = name;
            this.meta = meta;
        }
    }
}
