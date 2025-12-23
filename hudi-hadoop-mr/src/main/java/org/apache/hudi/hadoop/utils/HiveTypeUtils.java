/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.avro.InstanceCache;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.schema.HoodieSchemaType.ARRAY;
import static org.apache.hudi.common.schema.HoodieSchemaType.BOOLEAN;
import static org.apache.hudi.common.schema.HoodieSchemaType.BYTES;
import static org.apache.hudi.common.schema.HoodieSchemaType.DATE;
import static org.apache.hudi.common.schema.HoodieSchemaType.DECIMAL;
import static org.apache.hudi.common.schema.HoodieSchemaType.DOUBLE;
import static org.apache.hudi.common.schema.HoodieSchemaType.ENUM;
import static org.apache.hudi.common.schema.HoodieSchemaType.FIXED;
import static org.apache.hudi.common.schema.HoodieSchemaType.FLOAT;
import static org.apache.hudi.common.schema.HoodieSchemaType.INT;
import static org.apache.hudi.common.schema.HoodieSchemaType.LONG;
import static org.apache.hudi.common.schema.HoodieSchemaType.MAP;
import static org.apache.hudi.common.schema.HoodieSchemaType.NULL;
import static org.apache.hudi.common.schema.HoodieSchemaType.RECORD;
import static org.apache.hudi.common.schema.HoodieSchemaType.STRING;
import static org.apache.hudi.common.schema.HoodieSchemaType.TIME;
import static org.apache.hudi.common.schema.HoodieSchemaType.TIMESTAMP;
import static org.apache.hudi.common.schema.HoodieSchemaType.UNION;

/**
 * Convert an Avro Schema to a Hive TypeInfo
 * Taken from https://github.com/apache/hive/blob/rel/release-2.3.4/serde/src/java/org/apache/hadoop/hive/serde2/avro/SchemaToTypeInfo.java
 */
public class HiveTypeUtils {
  // Conversion of Avro primitive types to Hive primitive types
  // Avro             Hive
  // Null
  // boolean          boolean    check
  // int              int        check
  // long             bigint     check
  // float            double     check
  // double           double     check
  // bytes            binary     check
  // fixed            binary     check
  // string           string     check
  //                  tinyint
  //                  smallint

  // Map of Avro's primitive types to Hives (for those that are supported by both)
  private static final Map<HoodieSchemaType, TypeInfo> PRIMITIVE_TYPE_TO_TYPE_INFO = initTypeMap();
  private static Map<HoodieSchemaType, TypeInfo> initTypeMap() {
    Map<HoodieSchemaType, TypeInfo> theMap = new Hashtable<>();
    theMap.put(NULL, TypeInfoFactory.getPrimitiveTypeInfo("void"));
    theMap.put(BOOLEAN, TypeInfoFactory.getPrimitiveTypeInfo("boolean"));
    theMap.put(INT, TypeInfoFactory.getPrimitiveTypeInfo("int"));
    theMap.put(LONG, TypeInfoFactory.getPrimitiveTypeInfo("bigint"));
    theMap.put(FLOAT, TypeInfoFactory.getPrimitiveTypeInfo("float"));
    theMap.put(DOUBLE, TypeInfoFactory.getPrimitiveTypeInfo("double"));
    theMap.put(BYTES, TypeInfoFactory.getPrimitiveTypeInfo("binary"));
    theMap.put(FIXED, TypeInfoFactory.getPrimitiveTypeInfo("binary"));
    theMap.put(STRING, TypeInfoFactory.getPrimitiveTypeInfo("string"));
    return Collections.unmodifiableMap(theMap);
  }

  /**
   * Generate a list of of TypeInfos from an Avro schema.  This method is
   * currently public due to some weirdness in deserializing unions, but
   * will be made private once that is resolved.
   * @param schema Schema to generate field types for
   * @return List of TypeInfos, each element of which is a TypeInfo derived
   *         from the schema.
   * @throws AvroSerdeException for problems during conversion.
   */
  public static List<TypeInfo> generateColumnTypes(HoodieSchema schema) throws AvroSerdeException {
    return generateColumnTypes(schema, null);
  }

  /**
   * Generate a list of of TypeInfos from an Avro schema.  This method is
   * currently public due to some weirdness in deserializing unions, but
   * will be made private once that is resolved.
   * @param schema Schema to generate field types for
   * @param seenSchemas stores schemas processed in the parsing done so far,
   *         helping to resolve circular references in the schema
   * @return List of TypeInfos, each element of which is a TypeInfo derived
   *         from the schema.
   * @throws AvroSerdeException for problems during conversion.
   */
  public static List<TypeInfo> generateColumnTypes(HoodieSchema schema,
                                                   Set<HoodieSchema> seenSchemas) throws AvroSerdeException {
    List<HoodieSchemaField> fields = schema.getFields();

    List<TypeInfo> types = new ArrayList<TypeInfo>(fields.size());

    for (HoodieSchemaField field : fields) {
      types.add(generateTypeInfo(field.schema(), seenSchemas));
    }

    return types;
  }

  static InstanceCache<HoodieSchema, TypeInfo> typeInfoCache = new InstanceCache<HoodieSchema, TypeInfo>() {
    @Override
    protected TypeInfo makeInstance(HoodieSchema s,
                                    Set<HoodieSchema> seenSchemas)
        throws AvroSerdeException {
      return generateTypeInfoWorker(s, seenSchemas);
    }
  };

  /**
   * Convert an Avro Schema into an equivalent Hive TypeInfo.
   * @param schema to record. Must be of record type.
   * @param seenSchemas stores schemas processed in the parsing done so far,
   *         helping to resolve circular references in the schema
   * @return TypeInfo matching the Avro schema
   * @throws AvroSerdeException for any problems during conversion.
   */
  public static TypeInfo generateTypeInfo(HoodieSchema schema,
                                          Set<HoodieSchema> seenSchemas) throws AvroSerdeException {
    // For bytes type, it can be mapped to decimal.
    HoodieSchemaType type = schema.getType();
    if (type == DECIMAL && AvroSerDe.DECIMAL_TYPE_NAME
        .equalsIgnoreCase((String) schema.getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
      HoodieSchema.Decimal decimalSchema = (HoodieSchema.Decimal) schema;
      int precision = decimalSchema.getPrecision();
      int scale = decimalSchema.getScale();
      try {
        HiveDecimalUtils.validateParameter(precision, scale);
      } catch (Exception ex) {
        throw new AvroSerdeException("Invalid precision or scale for decimal type", ex);
      }
      return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
    }

    if (type == STRING
        && AvroSerDe.CHAR_TYPE_NAME.equalsIgnoreCase((String) schema.getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
      int maxLength = 0;
      try {
        maxLength = getIntFromSchema(schema, AvroSerDe.AVRO_PROP_MAX_LENGTH);
      } catch (Exception ex) {
        throw new AvroSerdeException("Failed to obtain maxLength value from file schema: " + schema, ex);
      }
      return TypeInfoFactory.getCharTypeInfo(maxLength);
    }

    if (type == STRING && AvroSerDe.VARCHAR_TYPE_NAME
        .equalsIgnoreCase((String) schema.getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
      int maxLength = 0;
      try {
        maxLength = getIntFromSchema(schema, AvroSerDe.AVRO_PROP_MAX_LENGTH);
      } catch (Exception ex) {
        throw new AvroSerdeException("Failed to obtain maxLength value from file schema: " + schema, ex);
      }
      return TypeInfoFactory.getVarcharTypeInfo(maxLength);
    }

    if (type == DATE
        && AvroSerDe.DATE_TYPE_NAME.equals(schema.getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE))) {
      return TypeInfoFactory.dateTypeInfo;
    }

    if (type == TIMESTAMP) {
      String avroLogicalType = (String) schema.getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE);
      switch (avroLogicalType) {
        case AvroSerDe.TIMESTAMP_TYPE_NAME:
          // case of timestamp-millis:
          return TypeInfoFactory.timestampTypeInfo;
        case "timestamp-micros":
          return TypeInfoFactory.longTypeInfo;
        default:
          throw new AvroSerdeException("Unsupported logical type found when evaluating TIMESTAMP type: " + avroLogicalType);
      }
    }

    if (type == TIME) {
      String avroLogicalType = (String) schema.getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE);
      switch ((String) schema.getProp(AvroSerDe.AVRO_PROP_LOGICAL_TYPE)) {
        case "time-millis":
          return TypeInfoFactory.intTypeInfo;
        case "time-micros":
          return TypeInfoFactory.longTypeInfo;
        default:
          throw new AvroSerdeException("Unsupported logical type found when evaluating TIME type: " + avroLogicalType);
      }
    }

    return typeInfoCache.retrieve(schema, seenSchemas);
  }

  private static boolean isEmpty(final CharSequence cs) {
    return cs == null || cs.length() == 0;
  }

  // added this from StringUtils
  private static boolean isNumeric(final CharSequence cs) {
    if (isEmpty(cs)) {
      return false;
    }
    final int sz = cs.length();
    for (int i = 0; i < sz; i++) {
      if (!Character.isDigit(cs.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  // added this from hive latest
  private static int getIntValue(Object obj) {
    int value = 0;
    if (obj instanceof Integer) {
      value = (int) obj;
    } else if (obj instanceof String && isNumeric((String)obj)) {
      value = Integer.parseInt((String)obj);
    }
    return value;
  }

  // added this from AvroSerdeUtils in hive latest
  public static int getIntFromSchema(HoodieSchema schema, String name) {
    Object obj = schema.getObjectProps().get(name);
    if (obj instanceof String) {
      return Integer.parseInt((String) obj);
    } else if (obj instanceof Integer) {
      return (int) obj;
    } else {
      throw new IllegalArgumentException("Expect integer or string value from property " + name
          + " but found type " + obj.getClass().getName());
    }
  }

  private static TypeInfo generateTypeInfoWorker(HoodieSchema schema,
                                                 Set<HoodieSchema> seenSchemas) throws AvroSerdeException {
    // HoodieSchema requires NULLable types to be defined as unions of some type T
    // and NULL.  This is annoying and we're going to hide it from the user.
    if (schema.isNullable()) {
      return generateTypeInfo(schema.getNonNullType(), seenSchemas);
    }

    HoodieSchemaType type = schema.getType();
    if (PRIMITIVE_TYPE_TO_TYPE_INFO.containsKey(type)) {
      return PRIMITIVE_TYPE_TO_TYPE_INFO.get(type);
    }

    switch (type) {
      case RECORD: return generateRecordTypeInfo(schema, seenSchemas);
      case MAP:    return generateMapTypeInfo(schema, seenSchemas);
      case ARRAY:  return generateArrayTypeInfo(schema, seenSchemas);
      case UNION:  return generateUnionTypeInfo(schema, seenSchemas);
      case ENUM:   return generateEnumTypeInfo(schema);
      default:     throw new AvroSerdeException("Do not yet support: " + schema);
    }
  }

  private static TypeInfo generateRecordTypeInfo(HoodieSchema schema,
                                                 Set<HoodieSchema> seenSchemas) throws AvroSerdeException {
    ValidationUtils.checkArgument(schema.getType() == RECORD, () -> schema + " is not a RECORD");

    if (seenSchemas == null) {
      seenSchemas = Collections.newSetFromMap(new IdentityHashMap<>());
    } else if (seenSchemas.contains(schema)) {
      throw new AvroSerdeException(
          "Recursive schemas are not supported. Recursive schema was " + schema
              .getFullName());
    }
    seenSchemas.add(schema);

    List<HoodieSchemaField> fields = schema.getFields();
    List<String> fieldNames = new ArrayList<>(fields.size());
    List<TypeInfo> typeInfos = new ArrayList<>(fields.size());

    for (int i = 0; i < fields.size(); i++) {
      fieldNames.add(i, fields.get(i).name());
      typeInfos.add(i, generateTypeInfo(fields.get(i).schema(), seenSchemas));
    }

    return TypeInfoFactory.getStructTypeInfo(fieldNames, typeInfos);
  }

  /**
   * Generate a TypeInfo for an Avro Map.  This is made slightly simpler in that
   * Avro only allows maps with strings for keys.
   */
  private static TypeInfo generateMapTypeInfo(HoodieSchema schema,
                                              Set<HoodieSchema> seenSchemas) throws AvroSerdeException {
    ValidationUtils.checkArgument(schema.getType() == MAP, () -> schema + " is not MAP");
    HoodieSchema valueType = schema.getValueType();
    TypeInfo ti = generateTypeInfo(valueType, seenSchemas);

    return TypeInfoFactory.getMapTypeInfo(TypeInfoFactory.getPrimitiveTypeInfo("string"), ti);
  }

  private static TypeInfo generateArrayTypeInfo(HoodieSchema schema,
                                                Set<HoodieSchema> seenSchemas) throws AvroSerdeException {
    ValidationUtils.checkArgument(schema.getType() == ARRAY, () -> schema + " is not an ARRAY");
    HoodieSchema itemsType = schema.getElementType();
    TypeInfo itemsTypeInfo = generateTypeInfo(itemsType, seenSchemas);

    return TypeInfoFactory.getListTypeInfo(itemsTypeInfo);
  }

  private static TypeInfo generateUnionTypeInfo(HoodieSchema schema,
                                                Set<HoodieSchema> seenSchemas) throws AvroSerdeException {
    ValidationUtils.checkArgument(schema.getType() == UNION, () -> schema + "is not a UNION");
    List<HoodieSchema> types = schema.getTypes();

    List<TypeInfo> typeInfos = new ArrayList<>(types.size());

    for (HoodieSchema type : types) {
      typeInfos.add(generateTypeInfo(type, seenSchemas));
    }

    return TypeInfoFactory.getUnionTypeInfo(typeInfos);
  }

  // Hive doesn't have an Enum type, so we're going to treat them as Strings.
  // During the deserialize/serialize stage we'll check for enumness and
  // convert as such.
  private static TypeInfo generateEnumTypeInfo(HoodieSchema schema) {
    ValidationUtils.checkArgument(schema.getType() == ENUM, () -> schema + " is not an ENUM");

    return TypeInfoFactory.getPrimitiveTypeInfo("string");
  }
}