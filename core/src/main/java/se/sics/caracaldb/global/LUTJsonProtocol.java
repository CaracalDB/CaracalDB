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
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.larskroll.common.ByteArrayFormatter;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.sics.caracaldb.Address;
import se.sics.caracaldb.Key;

/**
 *
 * @author lkroll
 */
public class LUTJsonProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(LUTJsonProtocol.class);

    static final Gson GSON = new GsonBuilder().setPrettyPrinting()
            .registerTypeAdapter(Key.class, new KeyAdapter())
            .registerTypeAdapter(Address.class, new AddressAdapter())
            .create();
    static final Charset CHARSET = Charset.forName("UTF8");

    static List<SchemaObj> prepareSchemas(SchemaData schemas) {
        LinkedList<SchemaObj> schemata = new LinkedList<SchemaObj>();
        for (Map.Entry<ByteBuffer, String> e : schemas.schemaNames.entrySet()) {
            ByteBuffer id = e.getKey();
            String name = e.getValue();
            ImmutableMap<String, String> meta = schemas.metaData.get(id);
            SchemaObj so = new SchemaObj();
            so.name = name;
            so.meta.putAll(meta);
            so.meta.put("id", ByteArrayFormatter.toHexString(id.array()));
            schemata.add(so);
        }
        return schemata;
    }

    public static String getSchemas(SchemaData schemas) {
        List<SchemaObj> schemata = prepareSchemas(schemas);
        return GSON.toJson(schemata);
    }

    static Map<Integer, Address> prepareHosts(ArrayList<Address> hosts) {
        Map<Integer, Address> hostMap = new LinkedHashMap<Integer, Address>();
        for (int i = 0; i < hosts.size(); i++) {
            hostMap.put(i, hosts.get(i));
        }
        return hostMap;
    }

    public static String getHosts(ArrayList<Address> hosts) {
        return GSON.toJson(prepareHosts(hosts));
    }

    static Map<Integer, RepSetObj> prepareRepSets(ArrayList<Integer[]> replicationSets, ArrayList<Integer> replicationSetVersions) {
        Map<Integer, RepSetObj> repSets = new LinkedHashMap<Integer, RepSetObj>();
        for (int i = 0; i < replicationSets.size(); i++) {
            RepSetObj rso = new RepSetObj();
            rso.version = replicationSetVersions.get(i);
            rso.members = replicationSets.get(i);
            repSets.put(i, rso);
        }
        return repSets;
    }

    public static String getRepSets(ArrayList<Integer[]> replicationSets, ArrayList<Integer> replicationSetVersions) {
        return GSON.toJson(prepareRepSets(replicationSets, replicationSetVersions));
    }

    static LookupGroupObj prepareLookupGroup(LookupGroup lg, long version) {
        LookupGroupObj lgo = new LookupGroupObj();
        lgo.version = version;
        lgo.prefix = new Key(lg.prefix);
        lgo.repGroups = lg.fillMap(new LinkedHashMap<Key, Integer>());
        return lgo;
    }

    static List<LookupGroupObj> prepareLookupGroups(LookupGroup[] virtualHostGroups, Long[] virtualHostGroupVersions) {
        List<LookupGroupObj> lgos = new LinkedList<LookupGroupObj>();
        for (int i = 0; i < virtualHostGroups.length; i++) {
            lgos.add(prepareLookupGroup(virtualHostGroups[i], virtualHostGroupVersions[i]));
        }
        return lgos;
    }

    public static String getLookupGroup(LookupGroup lg, long version) {
        return GSON.toJson(prepareLookupGroup(lg, version));
    }

    public static String getLookupGroups(LookupGroup[] virtualHostGroups, Long[] virtualHostGroupVersions) {
        List<LookupGroupObj> lgos = prepareLookupGroups(virtualHostGroups, virtualHostGroupVersions);
        return GSON.toJson(lgos);
    }

    static LUTObj prepareLUT(long version, SchemaData schemas,
            ArrayList<Address> hosts,
            ArrayList<Integer[]> replicationSets,
            ArrayList<Integer> replicationSetVersions,
            LookupGroup[] virtualHostGroups,
            Long[] virtualHostGroupVersions) {
        LUTObj luto = new LUTObj();
        luto.version = version;
        luto.schemas = prepareSchemas(schemas);
        luto.hosts = prepareHosts(hosts);
        luto.repSets = prepareRepSets(replicationSets, replicationSetVersions);
        luto.lookupGroups = prepareLookupGroups(virtualHostGroups, virtualHostGroupVersions);
        return luto;
    }

    public static String getLUT(long version, SchemaData schemas,
            ArrayList<Address> hosts,
            ArrayList<Integer[]> replicationSets,
            ArrayList<Integer> replicationSetVersions,
            LookupGroup[] virtualHostGroups,
            Long[] virtualHostGroupVersions) {
        return GSON.toJson(prepareLUT(version, schemas, hosts, replicationSets,
                replicationSetVersions, virtualHostGroups, virtualHostGroupVersions));
    }

    static class SchemaObj {

        String name;
        HashMap<String, String> meta = new HashMap<String, String>();

        public SchemaObj() {

        }

        @Override
        public String toString() {
            return name + ": " + meta;
        }
    }

    static class RepSetObj {

        int version;
        Integer[] members;
    }

    static class LookupGroupObj {

        long version;
        Key prefix;
        Map<Key, Integer> repGroups;

    }

    static class LUTObj {

        long version;
        List<SchemaObj> schemas;
        Map<Integer, Address> hosts;
        Map<Integer, RepSetObj> repSets;
        List<LookupGroupObj> lookupGroups;
    }

    public static class KeyAdapter extends TypeAdapter<Key> {

        @Override
        public void write(JsonWriter writer, Key value) throws IOException {
            if (value == null) {
                writer.nullValue();
                return;
            }
            writer.value(ByteArrayFormatter.storeFormat(value.getArray()));
        }

        @Override
        public Key read(JsonReader reader) throws IOException {
            if (reader.peek() == JsonToken.NULL) {
                reader.nextNull();
                return null;
            }
            String sformat = reader.nextString();
            return new Key(ByteArrayFormatter.parseStoreFormat(sformat));
        }

    }

    public static class AddressAdapter extends TypeAdapter<Address> {

        @Override
        public void write(JsonWriter writer, Address value) throws IOException {
            if (value == null) {
                writer.nullValue();
                return;
            }
            writer.value(value.toString());
        }

        @Override
        public Address read(JsonReader reader) throws IOException {
            if (reader.peek() == JsonToken.NULL) {
                reader.nextNull();
                return null;
            }
            String sformat = reader.nextString();
            String[] inetkey = sformat.split("/");
            String[] ipport = inetkey[0].split(":");
            InetAddress ip = InetAddress.getByName(ipport[0]);
            int port = Integer.parseInt(ipport[1]);
            if (inetkey.length == 2) {
                byte[] id = ByteArrayFormatter.parseStoreFormat(inetkey[1]);
                return new Address(ip, port, id);
            } else {
                return new Address(ip, port, null);
            }
        }
    }
}
