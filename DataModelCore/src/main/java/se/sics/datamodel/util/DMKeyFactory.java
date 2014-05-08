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
package se.sics.datamodel.util;

import com.google.common.io.Closer;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
//TYPE METADATA
//dbId.1.typeId                                 - typeKey  - <key, typeMetadata>
//DATA
//dbId.3.typeId.objNrId                         - DataKey  - <key, data>
//INDEXES
//dbId.4.typeId.idxId.idxVal.objNrId - indexKey - <key, "">
public class DMKeyFactory {

    public static final int MAX_INDEXVAL_SIZE = 127;
    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final byte[] MAX_IDXVAL;

    private static final byte STRING_END = (byte) 255;
    private static final byte INDEXVAL_INT = (byte) 0;
    private static final byte INDEXVAL_LONG = (byte) 1;
    private static final byte INDEXVAL_STRING = (byte) 2;

    static {
        byte[] maxString = new byte[127];
        Arrays.fill(maxString, (byte) 255);
        MAX_IDXVAL = maxString;
    }

    static final byte typeKF = (byte) 1;
//    static final byte tmFieldKF = (byte) 2;
    static final byte dataKF = (byte) 3;
    static final byte indexKF = (byte) 4;

    //data model key serialization into Key
    public static Key getTypeKey(ByteId dbId, ByteId typeId) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            w.write(dbId.getId());
            w.writeByte(typeKF);
            w.write(typeId.getId());
            w.flush();

            return new Key(baos.toByteArray());
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

//    public static Key getTMFieldKey(ByteId dbId, ByteId typeId, ByteId fieldId) throws IOException {
//        Closer closer = Closer.create();
//        try {
//            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
//            DataOutputStream w = closer.register(new DataOutputStream(baos));
//
//            w.write(dbId.getId());
//            w.writeByte(tmFieldKF);
//            w.write(typeId.getId());
//            w.write(fieldId.getId());
//            w.flush();
//
//            return new Key(baos.toByteArray());
//        } catch (Throwable e) {
//            throw closer.rethrow(e);
//        } finally {
//            closer.close();
//        }
//    }

    public static Key getDataKey(ByteId dbId, ByteId typeId, ByteId objNrId) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            w.write(dbId.getId());
            w.writeByte(dataKF);
            w.write(typeId.getId());
            w.write(objNrId.getId());
            w.flush();

            return new Key(baos.toByteArray());
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    // make sure that a compareTo b <=> a.serialized compareTo b.serialized
    //indexVal is prepended with a byte indexType: 0 for int, 1 for long, 2 for string
    public static Key getIndexKey(ByteId dbId, ByteId typeId, ByteId indexId, Object indexValue, ByteId objNrId) throws IOException {
        byte[] idxVal;
        if (indexValue instanceof Integer) {
            idxVal = serializeLexico((Integer) indexValue);
        } else if (indexValue instanceof Long) {
            idxVal = serializeLexico((Long) indexValue);
        } else if (indexValue instanceof String) {
            idxVal = serializeLexico((String) indexValue);
//        } else if (indexValue instanceof byte[]) {
//            idxVal = (byte[]) indexValue;
//            if (idxVal.length > MAX_INDEXVAL_SIZE) {
//                throw new IOException("KeyFactory - indexValue max 127 bytes");
//            }
        } else {
            throw new IOException("KeyFactory - indexValue is not of type int/long/String");
        }
        return getIndexKeyByte(dbId, typeId, indexId, idxVal, objNrId);
    }

    private static Key getIndexKeyByte(ByteId dbId, ByteId typeId, ByteId indexId, byte[] indexValue, ByteId objNrId) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            w.write(dbId.getId());
            w.writeByte(indexKF);
            w.write(typeId.getId());
            w.write(indexId.getId());
            w.write(indexValue);
            w.write(objNrId.getId());
            w.flush();

            return new Key(baos.toByteArray());
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    //data model key deserialization int KeyComponents
    public static DMKeyComponents getKeyComponents(Key key) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayInputStream bais = closer.register(new ByteArrayInputStream(key.getArray()));
            DataInputStream r = closer.register(new DataInputStream(bais));
            
            ByteId dbId = deserializeByteId(r);
            byte keyType = r.readByte();
            ByteId typeId = deserializeByteId(r);

            if (keyType == DMKeyFactory.typeKF) {
                return new TypeKeyComp(dbId, typeId);
//            } else if (keyType == DMKeyFactory.tmFieldKF) {
//                readTMFieldKey(r, dbId, typeId);
            } else if (keyType == DMKeyFactory.dataKF) {
                return readDataKey(r, dbId, typeId);
            } else if (keyType == DMKeyFactory.indexKF) {
                return readIndexKey(r, dbId, typeId);
            } else {
                throw new IOException("getKeyComponents - unknown type of key");
            }
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static DMKeyComponents readTMFieldKey(DataInputStream r, ByteId dbId, ByteId typeId) throws IOException {
        ByteId fieldId = deserializeByteId(r);
        return new TMFieldKeyComp(dbId, typeId, fieldId);
    }

    private static DMKeyComponents readDataKey(DataInputStream r, ByteId dbId, ByteId typeId) throws IOException {
        ByteId objId = deserializeByteId(r);
        return new TMFieldKeyComp(dbId, typeId, objId);
    }

    private static DMKeyComponents readIndexKey(DataInputStream r, ByteId dbId, ByteId typeId) throws IOException {
        ByteId indexId = deserializeByteId(r);
        Object indexVal = deserializeIndexVal(r);
        ByteId objId = deserializeByteId(r);
        
        return new IndexKeyComp(dbId, typeId, indexId, indexVal, objId);
    }
    
    //serialize/deserialize helper methods
    private static ByteId deserializeByteId(DataInputStream r) throws IOException {
        int byteIdSize = r.readByte();
        byte[] b_byteId = new byte[byteIdSize+1];
        b_byteId[0] = (byte) byteIdSize;
        r.read(b_byteId, 1, byteIdSize);
        return new ByteId(b_byteId);
    }
    
    private static Object deserializeIndexVal(DataInputStream r) throws IOException {
        byte b = r.readByte();

        if (b == INDEXVAL_INT) {
            return deserializeLexicoInt(r);
        } else if (b == INDEXVAL_LONG) {
            return deserializeLexicoLong(r);
        } else if (b == INDEXVAL_STRING) {
            return deserializeLexicoString(r);
        } else {
            throw new IOException("deserializeIndexVal - unknown value type");
        }
    }

    private static byte[] serializeLexico(int val) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            byte sign = (val < 0 ? (byte) 0 : (byte) 1);
            int absVal = Math.abs(val);
            byte[] iVal = Ints.toByteArray(absVal);

            w.write(INDEXVAL_INT);
            w.write(sign);
            w.write(iVal);

            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static Integer deserializeLexicoInt(DataInputStream r) throws IOException {
        byte b_sign = r.readByte();
        byte[] b_int = new byte[4];
        r.read(b_int);
        Integer val = Ints.fromByteArray(b_int);
        if (b_sign == (byte) 0) { //negative
            val = val * (-1);
        }
        return val;
    }

    private static byte[] serializeLexico(long val) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            byte sign = (val < 0 ? (byte) 0 : (byte) 1);
            long absVal = Math.abs(val);
            byte[] iVal = Longs.toByteArray(absVal);

            w.write(INDEXVAL_LONG);
            w.write(sign);
            w.write(iVal);

            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static Long deserializeLexicoLong(DataInputStream r) throws IOException {
        byte b_sign = r.readByte();
        byte[] b_long = new byte[8];
        r.read(b_long);
        Long val = Longs.fromByteArray(b_long);
        if (b_sign == (byte) 0) { //negative
            val = val * (-1);
        }
        return val;
    }

    private static byte[] serializeLexico(String val) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));
            if (val.length() > MAX_INDEXVAL_SIZE) {
                throw new IOException("KeyFactory - indexValue max 127 chars");
            }
            w.write(INDEXVAL_STRING); //string
            w.write(val.getBytes("UTF8"));
            w.write(STRING_END); //stop
            w.flush();

            return baos.toByteArray();
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    private static String deserializeLexicoString(DataInputStream r) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            do {
                byte b = r.readByte();
                if (b == STRING_END) {
                    break;
                }
                baos.write(b);
            } while (true);
            w.flush();

            return new String(baos.toByteArray(), "UTF8");
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

    //*****Ranges*****
    // ["dbId.1.byteIdMin","dbId.1.byteIdMax"]
    public static KeyRange getAllTypesRange(ByteId dbId) throws IOException {
        Key begin = getTypeKey(dbId, ByteIdFactory.MIN_BYTE_ID);
        Key end = getTypeKey(dbId, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).closed(end);
        return range;
    }

//    // ["dbId.2.typeId.byteIdMin","dbId.2.typeId.byteIdMax"]
//    public static KeyRange getTMRange(ByteId dbId, ByteId typeId) throws IOException {
//        Key begin = getTMFieldKey(dbId, typeId, ByteIdFactory.MIN_BYTE_ID);
//        Key end = getTMFieldKey(dbId, typeId, ByteIdFactory.MAX_BYTE_ID);
//        KeyRange range = KeyRange.closed(begin).closed(end);
//        return range;
//    }

    // ["dbId.4.typeId.idxId.idxVal.byteIdMin", "dbId.4.typeId.idxId.idxVal.byteIdMax"]
    public static KeyRange getIndexRangeIS(ByteId dbId, ByteId typeId, ByteId idxId, Object idxVal) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MIN_BYTE_ID);
        Key end = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).closed(end);
        return range;
    }
    
    // ["dbId.4.typeId.idxId.idxVal1.byteIdMin", "dbId.4.typeId.idxId.idxVal2.byteIdMax"]
    public static KeyRange getIndexRangeBetween(ByteId dbId, ByteId typeId, ByteId idxId, Object idxVal1, Object idxVal2) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, idxVal1, ByteIdFactory.MIN_BYTE_ID);
        Key end = getIndexKey(dbId, typeId, idxId, idxVal2, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).closed(end);
        return range;
    }

    // ["dbId.4.typeId.idxId.idxValMin.byteIdMin", "dbId.4.typeId.idxId.idxVal.byteIdMax"]
    public static KeyRange getIndexRangeLTE(ByteId dbId, ByteId typeId, ByteId idxId, Object idxVal) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, new byte[]{(byte) 0}, ByteIdFactory.MIN_BYTE_ID);
        Key end = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).closed(end);
        return range;
    }

    // ["dbId.4.typeId.idxId.idxValMin.byteIdMin", "dbId.4.typeId.idxId.idxVal.byteIdMin")
    public static KeyRange getIndexRangeLT(ByteId dbId, ByteId typeId, ByteId idxId, Object idxVal) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, new byte[]{(byte) 0}, ByteIdFactory.MIN_BYTE_ID);
        Key end = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MIN_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).open(end);
        return range;
    }

    // ["dbId.4.typeId.idxId.idxVal.byteIdMin", "dbId.4.typeId.idxId.idxValMax.byteIdMax"]
    public static KeyRange getIndexRangeGTE(ByteId dbId, ByteId typeId, ByteId idxId, Object idxVal) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MIN_BYTE_ID);
        Key end = getIndexKey(dbId, typeId, idxId, MAX_IDXVAL, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).closed(end);
        return range;
    }

    // ("dbId.typeId.idxId.idxVal.byteIdMax", "dbId.typeId.idxId.idxValMax.byteIdMax"]
    public static KeyRange getIndexRangeGT(ByteId dbId, ByteId typeId, ByteId idxId, Object idxVal) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MAX_BYTE_ID);
        Key end = getIndexKey(dbId, typeId, idxId, MAX_IDXVAL, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.open(begin).closed(end);
        return range;
    }

    public static interface DMKeyComponents {
    }

    public static class TypeKeyComp implements DMKeyComponents {

        public final ByteId dbId;
        public final ByteId typeId;

        public TypeKeyComp(ByteId dbId, ByteId typeId) {
            this.dbId = dbId;
            this.typeId = typeId;
        }
    }

    public static class TMFieldKeyComp implements DMKeyComponents {

        public final ByteId dbId;
        public final ByteId typeId;
        public final ByteId fieldId;

        public TMFieldKeyComp(ByteId dbId, ByteId typeId, ByteId fieldId) {
            this.dbId = dbId;
            this.typeId = typeId;
            this.fieldId = fieldId;
        }
    }

    public static class DataKeyComp implements DMKeyComponents {

        public final ByteId dbId;
        public final ByteId typeId;
        public final ByteId objId;

        public DataKeyComp(ByteId dbId, ByteId typeId, ByteId objId) {
            this.dbId = dbId;
            this.typeId = typeId;
            this.objId = objId;
        }
    }

    public static class IndexKeyComp implements DMKeyComponents {

        public final ByteId dbId;
        public final ByteId typeId;
        public final ByteId indexId;
        public final Object indexValue;
        public final ByteId objId;

        public IndexKeyComp(ByteId dbId, ByteId typeId, ByteId indexId, Object indexValue, ByteId objId) {
            this.dbId = dbId;
            this.typeId = typeId;
            this.indexId = indexId;
            this.indexValue = indexValue;
            this.objId = objId;
        }
    }
}