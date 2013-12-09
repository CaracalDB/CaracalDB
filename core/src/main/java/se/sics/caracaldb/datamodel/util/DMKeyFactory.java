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
package se.sics.caracaldb.datamodel.util;

import com.google.common.io.Closer;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import se.sics.caracaldb.Key;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
//TYPE METADATA
//dbId.1.typeId              - typeKey        - <key, typeName>
//dbId.2.typeId.fieldId      - tmFieldKey     - <key, <fieldName, fieldTypem, indexed>>
//DATA
//dbId.3.typeId.objNrId    		      - fieldDataKey  - <key, data>
//INDEXES
//dbId.4.typeId.idxId.idxVal.objNrId          - idxValKey     - <key, "">
public class DMKeyFactory {

    static final byte typeKF = (byte) 1;
    static final byte tmFieldKF = (byte) 2;
    static final byte dataKF = (byte) 3;
    static final byte indexKF = (byte) 4;

//    {
//        Closer closer = Closer.create();
//        try {
//            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
//            DataOutputStream w = closer.register(new DataOutputStream(baos));
//
//            w.flush();
//
//            return new Key(baos.toByteArray());
//        } catch (Throwable e) {
//            throw closer.rethrow(e);
//        } finally {
//            closer.close();
//        }
//    }

    public static Key getTypeKey(int dbId, int typeId) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));
            
            w.writeInt(dbId);
            w.writeByte(typeKF);
            w.writeInt(typeId);
            w.flush();

            return new Key(baos.toByteArray());
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static Key getTMFieldKey(int dbId, int typeId, int fieldId) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));
            
            w.writeInt(dbId);
            w.writeByte(tmFieldKF);
            w.writeInt(typeId);
            w.writeInt(fieldId);
            w.flush();

            return new Key(baos.toByteArray());
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static Key getDataKey(int dbId, int typeId, long objNrId) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));
            
            w.writeInt(dbId);
            w.writeByte(dataKF);
            w.writeInt(typeId);
            w.writeLong(objNrId);
            w.flush();

            return new Key(baos.toByteArray());
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    public static Key getIndexKey(int dbId, int typeId, int indexId, byte[] indexValue, long objNrId) throws IOException {
        if (indexValue.length > 127) {
            throw new IOException("KeyFactory - indexValue max 127 bytes");
        }
        
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));
            
            w.writeInt(dbId);
            w.writeByte(indexKF);
            w.writeInt(typeId);
            w.writeInt(indexId);
            w.write(indexValue);
            w.writeLong(objNrId);
            w.flush();

            return new Key(baos.toByteArray());
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }
}
