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
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static se.sics.caracaldb.global.LUTJsonProtocol.GSON;
import se.sics.caracaldb.system.Configuration;
import com.larskroll.common.ByteArrayFormatter;
import se.sics.caracaldb.utils.HashIdGenerator;

/**
 *
 * @author lkroll
 */
public abstract class SchemaReader extends LUTJsonProtocol {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaReader.class);

    public static SchemaData importSchemas(Configuration config) {
        ImmutableSet<String> paths = config.getSchemaFiles();
        List<String> jsons = new LinkedList<String>();
        try {
            for (String path : paths) {
                // try as resource
                InputStream in = ClassLoader.getSystemResource(path).openStream();
                if (in == null) {
                    LOG.warn("Couldn't find resource '{}'!", path);
                    // try as file
                    try {
                        in = new FileInputStream(path);
                    } catch (FileNotFoundException ex) {
                        LOG.warn("Can't find given file: {}", ex);
                    } catch (SecurityException ex) {
                        LOG.warn("Not allowed to access file: {}", ex);
                    }
                    if (in == null) {
                        LOG.warn("Couldn't find file '{}'! Skipping...", path);
                        continue;
                    }
                }
                InputStreamReader reader = new InputStreamReader(in, CHARSET);
                BufferedReader breader = new BufferedReader(reader);
                try {
                    LOG.info("Importing Schema: {}", path);
                    StringBuilder sb = new StringBuilder();
                    while (breader.ready()) {
                        sb.append(breader.readLine());
                    }
                    jsons.add(sb.toString());
                } finally {
                    reader.close();
                    breader.close();
                    in.close();
                }

            }
        } catch (IOException ex) {
            LOG.error("While importing schemas: \n {}", ex);
        }
        return importSchemas(jsons, config.getIdGenerator());
    }

    public static SchemaData importSchemas(Collection<String> json, HashIdGenerator idGen) {

        Type collectionType = new TypeToken<LinkedList<SchemaObj>>() {
        }.getType();
        LinkedList<SchemaObj> schemata = new LinkedList<SchemaObj>();
        for (String jsonS : json) {
            LinkedList<SchemaObj> schemas = GSON.fromJson(jsonS, collectionType);
            schemata.addAll(schemas);
        }
        SchemaData sd = new SchemaData();
        sd.version = 0;
        for (SchemaObj so : schemata) {
            String idString = so.meta.get("id");
            ByteBuffer id = null;
            if (idString == null) {
                byte[] schemaId = idGen.idForNameDontStartWith(so.name, LookupTable.RESERVED_PREFIX.getArray());
                id = ByteBuffer.wrap(schemaId);
            } else {
                System.out.println("ID-String: " + idString);
                byte[] schemaId = ByteArrayFormatter.fromHexString(idString);
                id = ByteBuffer.wrap(schemaId);
            }
            so.meta.remove("id");
            sd.schemaIDs.put(so.name, id);
            sd.schemaNames.put(id, so.name);
            sd.metaData.put(id, ImmutableMap.copyOf(so.meta));
            System.out.println(so);
        }
        return sd;
    }

    public static String exportSchemas(SchemaData schemas, String filename) throws FileNotFoundException, UnsupportedEncodingException {
        List<SchemaObj> schemata = prepareSchemas(schemas);
        String json = GSON.toJson(schemata);
        System.out.println("SCHEMAS: \n " + json);
        if (filename != null) {
            PrintWriter writer = null;
            try {
                writer = new PrintWriter(filename, CHARSET.name());
                writer.print(json);
            } finally {
                if (writer != null) {
                    writer.close();
                }
            }
        }
        return json;
    }

}
