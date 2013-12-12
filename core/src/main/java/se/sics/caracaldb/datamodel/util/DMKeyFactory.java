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
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import se.sics.caracaldb.Key;
import se.sics.caracaldb.KeyRange;

/**
 * @author Alex Ormenisan <aaor@sics.se>
 */
//TYPE METADATA
//dbId.1.typeId                                 - typeKey        - <key, typeName>
//dbId.2.typeId.fieldId                         - tmFieldKey     - <key, <fieldName, fieldTypem, indexed>>
//DATA
//dbId.3.typeId.objNrId                         - DataKey  - <key, data>
//INDEXES
//dbId.4.typeId.idxId.idxVal.objNrId    - indexKey     - <key, "">
public class DMKeyFactory {
    public static final int MAX_INDEXVAL_SIZE = 127;
    public static final byte[] MAX_STRING;
    
    static {
        byte[] maxString = new byte[127];
        Arrays.fill(maxString, (byte)255);
        MAX_STRING = maxString;
    }

    static final byte typeKF = (byte) 1;
    static final byte tmFieldKF = (byte) 2;
    static final byte dataKF = (byte) 3;
    static final byte indexKF = (byte) 4;

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

    public static Key getTMFieldKey(ByteId dbId, ByteId typeId, ByteId fieldId) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));

            w.write(dbId.getId());
            w.writeByte(tmFieldKF);
            w.write(typeId.getId());
            w.write(fieldId.getId());
            w.flush();

            return new Key(baos.toByteArray());
        } catch (Throwable e) {
            throw closer.rethrow(e);
        } finally {
            closer.close();
        }
    }

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

    private static Key getIndexKey(ByteId dbId, ByteId typeId, ByteId indexId, byte[] indexValue, ByteId objNrId) throws IOException {
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
    
    // make sure that a compareTo b <=> a.serialized compareTo b.serialized
    public static Key getIndexKey(ByteId dbId, ByteId typeId, ByteId indexId, int indexValue, ByteId objNrId) throws IOException {
        byte[] idxVal = serializeLexico(indexValue);
        return getIndexKey(dbId, typeId, indexId, idxVal, objNrId);
    }
    
    private static byte[] serializeLexico(int val) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));
            
            byte sign = (val < 0 ? (byte) 0 : (byte)1); 
            int absVal = Math.abs(val);
            byte[] iVal = Ints.toByteArray(absVal);
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
    
    // make sure that a compareTo b <=> a.serialized compareTo b.serialized
    public static Key getIndexKey(ByteId dbId, ByteId typeId, ByteId indexId, long indexValue, ByteId objNrId) throws IOException {
        byte[] idxVal = serializeLexico(indexValue);
        return getIndexKey(dbId, typeId, indexId, idxVal, objNrId);
    }
    
    private static byte[] serializeLexico(long val) throws IOException {
        Closer closer = Closer.create();
        try {
            ByteArrayOutputStream baos = closer.register(new ByteArrayOutputStream());
            DataOutputStream w = closer.register(new DataOutputStream(baos));
            
            byte sign = (val < 0 ? (byte) 0 : (byte)1); 
            long absVal = Math.abs(val);
            byte[] iVal = Longs.toByteArray(absVal);
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
    
    // make sure that a compareTo b <=> a.serialized compareTo b.serialized
    public static Key getIndexKey(ByteId dbId, ByteId typeId, ByteId indexId, String indexValue, ByteId objNrId) throws IOException {
        if (indexValue.length() > MAX_INDEXVAL_SIZE) {
            throw new IOException("KeyFactory - indexValue max 127 bytes");
        }
        byte[] idxVal = indexValue.getBytes("UTF8");
        return getIndexKey(dbId, typeId, indexId, idxVal, objNrId);
    }
    
    //*****Ranges*****
    // ["dbId.1.byteIdMin","dbId.1.byteIdMax"]
    public static KeyRange getAllTypesRange(ByteId dbId) throws IOException {
        Key begin = getTypeKey(dbId, ByteIdFactory.MIN_BYTE_ID);
        Key end = getTypeKey(dbId, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).closed(end);
        return range;
    }

    // ["dbId.2.typeId.byteIdMin","dbId.2.typeId.byteIdMax"]
    public static KeyRange getTMRange(ByteId dbId, ByteId typeId) throws IOException {
        Key begin = getTMFieldKey(dbId, typeId, ByteIdFactory.MIN_BYTE_ID);
        Key end = getTMFieldKey(dbId, typeId, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).closed(end);
        return range;
    }
    
    public static KeyRange getIndexRange(ByteId dbId, ByteId typeId, ByteId idxId, int indexValue, IdxRangeType idxRangeType) throws IOException {
        
    }
    // ["dbId.4.typeId.idxId.idxVal.byteIdMin", "dbId.4.typeId.idxId.idxVal.byteIdMax"]
    public static KeyRange getIndexRangeIS(ByteId dbId, ByteId typeId, ByteId idxId, byte[] idxVal) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MIN_BYTE_ID);
        Key end = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).closed(end);
        return range;
    }

    // ["dbId.4.typeId.idxId.idxValMin.byteIdMin", "dbId.4.typeId.idxId.idxVal.byteIdMax"]
    public static KeyRange getIndexRangeLTE(ByteId dbId, ByteId typeId, ByteId idxId, byte[] idxVal) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, new byte[]{(byte) 0}, ByteIdFactory.MIN_BYTE_ID);
        Key end = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MAX_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).closed(end);
        return range;
    }

    // ["dbId.4.typeId.idxId.idxValMin.byteIdMin", "dbId.4.typeId.idxId.idxVal.byteIdMin")
    public static KeyRange getIndexRangeLT(ByteId dbId, ByteId typeId, ByteId idxId, byte[] idxVal) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, new byte[]{(byte) 0}, ByteIdFactory.MIN_BYTE_ID);
        Key end = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MIN_BYTE_ID);
        KeyRange range = KeyRange.closed(begin).open(end);
        return range;
    }

    // ["dbId.4.typeId.idxId.idxVal.byteIdMin", "dbId.4.typeId.idxId.idxValMax.byteIdMax"]
    public static KeyRange getIndexRangeGTE(ByteId dbId, ByteId typeId, ByteId idxId, byte[] idxVal) throws IOException {
        Key begin = getIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.MIN_BYTE_ID);
        Key end = getDataIndexKey(dbId, typeId, nextIdxId, new byte[]{(byte) 0}, ByteIdFactory.getMinByteId());

        Token beginT = TokenFactory.newToken(begin.getBytes());
        Token endT = TokenFactory.newToken(end.getBytes());
        Token.IntervalBounds bounds = Token.IntervalBounds.CLOSED_OPEN;
        return new GeneralRange(beginT, endT, bounds);
    }
//
//	/**
//	 * ("dbId.typeId.idxId.idxVal.byteIdMax", "dbId.typeId.idxId+1.idxValMin.byteIdMin")
//	 */
//	public static GeneralRange getIdxValueRangeGT(ByteId dbId, ByteId typeId, ByteId idxId, byte[] idxVal) throws BadInputException, IOException {
//		Key begin = getDataIndexKey(dbId, typeId, idxId, idxVal, ByteIdFactory.getMaxByteId());
//		ByteId nextIdxId;
//		try {
//			nextIdxId = ByteIdFactory.nextId(idxId);
//		} catch (BadInputException e) {
//			/*TODO Alex RuntimeException - to fix
//			 * if idxId is already maxId what to do
//			 */
//			throw new RuntimeException("KeyFactory - max idxId reached");
//		}
//		Key end = getDataIndexKey(dbId, typeId, nextIdxId, new byte[]{(byte)0}, ByteIdFactory.getMinByteId());
//
//		Token beginT = TokenFactory.newToken(begin.getBytes());
//		Token endT = TokenFactory.newToken(end.getBytes());
//		Token.IntervalBounds bounds = Token.IntervalBounds.OPEN_OPEN;
//		return new GeneralRange(beginT, endT, bounds);
//	}
//
//	/*****Keys Parser*****/
//	public static Key getKey(Token token) throws BadInputException {
//		if(!(token instanceof StringToken)) {
//			throw new BadInputException("Expected StringToken");
//		}
//		KeyComponents keyComponents = getKeyComponents(((StringToken)token).getBytes());
//		return new Key(((StringToken)token).getBytes(), keyComponents);
//	}
//
//	public static KeyComponents getKeyComponents(byte[] key) throws BadInputException {
//		ByteBuffer byteSource = ByteBuffer.wrap(key);
//
//		ByteId dbId = ByteIdParser.decode(byteSource);
//		byte keyFlag = getByte(byteSource);
//		if(keyFlag == typeKeyFlag) {
//			return getTypeKey(byteSource, dbId);
//		} else if(keyFlag == tmKeyFlag) {
//			return getTMKey(byteSource, dbId);
//		} else if(keyFlag == dataKeyFlag) {
//			return getDataKey(byteSource, dbId);
//		} else {
//			throw new BadInputException("Expected TypeKey / TMKey / DataKey");
//		}
//	}
//
//	private static KeyComponents getTypeKey(ByteBuffer byteSource, ByteId dbId) throws BadInputException {
//		ByteId typeId = ByteIdParser.decode(byteSource);
//		if(byteSource.hasRemaining()) {
//			throw new BadInputException("Source contained a TypeKey and more");
//		}
//		return new KeyComponents(new byte[]{typeKeyFlag}, ImmutableList.of(dbId, typeId));
//	}
//
//	private static KeyComponents getTMKey(ByteBuffer byteSource, ByteId dbId) throws BadInputException {
//		ByteId typeId = ByteIdParser.decode(byteSource);
//		byte keyFlag = getByte(byteSource);
//		if(keyFlag == tmFieldNameFlag) {
//			return getFieldNameKey(byteSource, dbId, typeId);
//		} else if(keyFlag == tmFieldTypeFlag) {
//			return getFieldTypeKey(byteSource, dbId, typeId);
//		} else if(keyFlag == tmIndexFlag) {
//			return getIndexIdKey(byteSource, dbId, typeId);
//		} else {
//			throw new BadInputException("Expected TypeKey");
//		}
//	}
//
//	private static KeyComponents getFieldNameKey(ByteBuffer byteSource, ByteId dbId, ByteId typeId) throws BadInputException {
//		ByteId fieldId = ByteIdParser.decode(byteSource);
//		if(byteSource.hasRemaining()) {
//			throw new BadInputException("Source contained a TMKey and more");
//		}
//		return new KeyComponents(new byte[]{tmKeyFlag, tmFieldNameFlag}, ImmutableList.of(dbId, typeId, fieldId));
//	}
//
//	private static KeyComponents getFieldTypeKey(ByteBuffer byteSource, ByteId dbId, ByteId typeId) throws BadInputException {
//		ByteId fieldId = ByteIdParser.decode(byteSource);
//		if(byteSource.hasRemaining()) {
//			throw new BadInputException("Source contained a TMKey and more");
//		}
//		return new KeyComponents(new byte[]{tmKeyFlag, tmFieldTypeFlag}, ImmutableList.of(dbId, typeId, fieldId));
//	}
//
//	private static KeyComponents getIndexIdKey(ByteBuffer byteSource, ByteId dbId, ByteId typeId) throws BadInputException {
//		ByteId indexId = ByteIdParser.decode(byteSource);
//		if(byteSource.hasRemaining()) {
//			throw new BadInputException("Source contained a TMKey and more");
//		}
//		return new KeyComponents(new byte[]{tmKeyFlag, tmIndexFlag}, ImmutableList.of(dbId, typeId, indexId));
//	}
//
//	private static KeyComponents getDataKey(ByteBuffer byteSource, ByteId dbId) throws BadInputException {
//		ByteId typeId = ByteIdParser.decode(byteSource);
//		byte keyFlag = getByte(byteSource);
//		if(keyFlag == dataObjectFlag) {
//			return getDataObjectKey(byteSource, dbId, typeId);
//		} else if(keyFlag == dataIndexFlag) {
//			return getDataIndexValueKey(byteSource, dbId, typeId);
//		} else {
//			throw new BadInputException("Expected DataKey");
//		}
//	}
//
//	private static KeyComponents getDataObjectKey(ByteBuffer byteSource, ByteId dbId, ByteId typeId) throws BadInputException {
//		ByteId objNrId = ByteIdParser.decode(byteSource);
//		if(byteSource.hasRemaining()) {
//			throw new BadInputException("Source contained a DataKey and more");
//		}
//		return new KeyComponents(new byte[]{dataKeyFlag, dataObjectFlag}, ImmutableList.of(dbId, typeId, objNrId));
//	}
//
//	private static KeyComponents getDataIndexValueKey(ByteBuffer byteSource, ByteId dbId, ByteId typeId) throws BadInputException {
//		ByteId indexId = ByteIdParser.decode(byteSource);
//		byte indexValueLength = getByte(byteSource);
//		byte[] indexValue = getByteArray(byteSource, indexValueLength);
//		ByteId objNrId = ByteIdParser.decode(byteSource);
//		if(byteSource.hasRemaining()) {
//			throw new BadInputException("Source contained a DataKey and more");
//		}
//		return new KeyComponents(new byte[]{dataKeyFlag, dataIndexFlag}, ImmutableList.of(dbId, typeId, indexId, objNrId), ImmutableList.of(indexValue));
//	}
//
//	/*****/
//	private static byte getByte(ByteBuffer byteSource) throws BadInputException {
//		int mark = byteSource.position();
//		try {
//			return byteSource.get();
//		} catch(BufferUnderflowException e) {
//			byteSource.position(mark);
//			throw new BadInputException(e);
//		}
//	}
//
//	private static byte[] getByteArray(ByteBuffer byteSource, int length) throws BadInputException {
//		int mark = byteSource.position();
//		try {
//			byte[] byteArray = new byte[length];
//			byteSource.get(byteArray);
//			return byteArray;
//		} catch(BufferUnderflowException e) {
//			byteSource.position(mark);
//			throw new BadInputException(e);
//		}
//	}
//}
    
    public static enum IdxRangeType {
        IS, LT, LTE, GT, GTE;
    }
}
