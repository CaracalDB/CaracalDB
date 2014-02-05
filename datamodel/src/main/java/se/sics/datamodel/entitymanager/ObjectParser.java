package se.sics.datamodel.entitymanager;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ObjectParser {

	public static TypeInfo getTypeInfo(ByteId dbId, Object obj) throws ParsingException {
		if (obj == null) {
			throw new ParsingException("typeInfo is null");
		}
		Class<?> objType = obj.getClass();
		if (objType.isAnnotationPresent(Entity.class)) {
			String typeName = objType.getCanonicalName();
			TypeInfo typeInfo = new TypeInfo(dbId, typeName);

			Field[] fieldSet = objType.getDeclaredFields();
			Method[] methodSet = objType.getDeclaredMethods();

			for (Field field : fieldSet) {
				String fieldName = field.getName().toLowerCase();
				String fieldType = field.getType().getSimpleName().toLowerCase();
				boolean setMethod = false;
				boolean getMethod = false;
				for (Method method : methodSet) {
					String methodName = method.getName().toLowerCase();
					if (methodName.equals("get" + fieldName)) {
						getMethod = true;
					} else if (methodName.equals("set" + fieldName)) {
						setMethod = true;
					}
				}
				if (!getMethod || !setMethod) {
					throw new ParsingException("set/get methods undefined for" + field.getName());
				}
				if (field.isAnnotationPresent(ObjId.class)) {
					//typeInfo.addId(fieldName);
				} else {
					try {
						typeInfo.addField(fieldName, fieldType);
					} catch (TypeInfo.TypeInfoException e) {
						throw new ParsingException("Unknown type", e);
					}
				}
				if (field.isAnnotationPresent(Index.class)) {
					typeInfo.addIndexedField(fieldName);
				}

			}
			return typeInfo;
		} else {
			throw new ParsingException("not an Entity object");
		}
	}

	public static ParsedDBObject getDBObject(Object obj, TypeInfo typeInfo) throws ParsingException {
		if (null == obj || null == typeInfo) {
			throw new ParsingException("obj or typeInfo are null");
		}
		Class<?> objType = obj.getClass();
		if (!objType.isAnnotationPresent(Entity.class)) {
			throw new ParsingException("not an Entity object");
		}
		if(!typeInfo.typeName.equals(objType.getCanonicalName())) {
			throw new ParsingException("obj type is not the same as the typeInfo");
		}

		Field[] fieldSet = obj.getClass().getDeclaredFields();
		ParsedDBObject dbObj = null;

		for (Field field : fieldSet) {
			if (field.isAnnotationPresent(ObjId.class)) {

				try {
					String fieldName = field.getName();
					String methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
					Method idGetMethod = obj.getClass().getDeclaredMethod(methodName);
					String objId = (String) idGetMethod.invoke(obj);
					ByteId objNrId = ByteIdFactory.getId(objId);
					dbObj = new ParsedDBObject(typeInfo, objNrId);
				} catch (NoSuchMethodException e) {
					throw new ParsingException("objId format exception", e);
				} catch (SecurityException e) {
					throw new ParsingException("objId format exception", e);
				} catch (IllegalArgumentException e) {
					throw new ParsingException("objId format exception", e);
				} catch (IllegalAccessException e) {
					throw new ParsingException("objId format exception", e);
				} catch (InvocationTargetException e) {
					throw new ParsingException("objId format exception", e);
				} catch (IndexOutOfBoundsException e) {
					throw new ParsingException("objId format exception", e);
				} catch (BadInputException e) {
					throw new ParsingException("objId format exception", e);
				}
				break;
			}
		}
		if (dbObj == null) {
			throw new ParsingException("no objId");
		}
		for (Field field : fieldSet) {
			if (!field.isAnnotationPresent(ObjId.class)) {
				String fieldName = field.getName();
				ByteId fieldId = typeInfo.getFieldId(fieldName.toLowerCase());
				try {
					String methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
					Method fieldGetMethod = obj.getClass().getDeclaredMethod(methodName);
					Object fieldVal = fieldGetMethod.invoke(obj);
					dbObj.putFieldVal(fieldId, fieldVal);
				} catch (IllegalArgumentException e) {
					throw new ParsingException("field format exception", e);
				} catch (InvocationTargetException e) {
					throw new ParsingException("field format exception", e);
				} catch (NoSuchMethodException e) {
					throw new ParsingException("field format exception", e);
				} catch (SecurityException e) {
					throw new ParsingException("field format exception", e);
				} catch (TypeInfo.TypeInfoException e) {
					throw new ParsingException("field format exception", e);
				} catch (IllegalAccessException e) {
					throw new ParsingException("field format exception", e);
				} catch (IndexOutOfBoundsException e) {
					throw new ParsingException("objId format exception", e);
				}
			}
		}
		return dbObj;
	}

	public static <T> T getObject(Class<T> objClass, TypeInfo typeInfo, ParsedDBObject dbObj) throws ParsingException {
		if (null == objClass || null == typeInfo || null == dbObj) {
			throw new ParsingException("one of: objClass/typeInfo/dbObj is null");
		}
		if (!objClass.isAnnotationPresent(Entity.class)) {
			throw new ParsingException("not an Entity object");
		}
		if(!typeInfo.typeName.equals(objClass.getCanonicalName())) {
			throw new ParsingException("obj type is not the same as the typeInfo");
		}

		try {
			T obj = objClass.newInstance();
			Method idSetMethod = objClass.getDeclaredMethod("setId", String.class);
			idSetMethod.invoke(obj, dbObj.objNrId.toString());

			Method[] methodSet = objClass.getDeclaredMethods();
			for (Method method : methodSet) {
				String methodName = method.getName();
				if (methodName.startsWith("set")) {
					if(methodName.equals("setId")) {
						continue;
					} else {
						String fieldName = methodName.substring("set".length()).toLowerCase();
						ByteId fieldId = typeInfo.getFieldId(fieldName);
						Object fieldVal = dbObj.getFieldVal(fieldId);
						method.invoke(obj, fieldVal);
					}
				}
			}
			return obj;
		} catch (IllegalArgumentException e) {
			throw new ParsingException("dbObject->object parsing exception", e);
		} catch (InvocationTargetException e) {
			throw new ParsingException("dbObject->object parsing exception", e);
		} catch (InstantiationException e) {
			throw new ParsingException("dbObject->object parsing exception", e);
		} catch (IllegalAccessException e) {
			throw new ParsingException("dbObject->object parsing exception", e);
		} catch (SecurityException e) {
			throw new ParsingException("dbObject->object parsing exception", e);
		} catch (NoSuchMethodException e) {
			throw new ParsingException("dbObject->object parsing exception", e);
		}
	}

	public static class ParsingException extends Exception {
		private static final long serialVersionUID = -1102144965842035649L;

		public ParsingException(String message) {
			super(message);
		}

		public ParsingException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
