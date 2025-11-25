/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.avro;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.avro.JsonProperties.NULL_VALUE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;

/**
 * <p>
 * Converts an Avro schema into a Parquet schema, or vice versa. See package
 * documentation for details of the mapping.
 * </p>
 * This was taken from parquet-java 1.13.1 AvroSchemaConverter and modified
 * to support local timestamp types by copying a few methods from 1.14.0 AvroSchemaConverter.
 */
@SuppressWarnings("all")
public class AvroSchemaConverterWithTimestampNTZ extends HoodieAvroParquetSchemaConverter {

  public static final String ADD_LIST_ELEMENT_RECORDS =
      "parquet.avro.add-list-element-records";
  private static final boolean ADD_LIST_ELEMENT_RECORDS_DEFAULT = true;

  private final boolean assumeRepeatedIsListElement;
  private final boolean writeOldListStructure;
  private final boolean writeParquetUUID;
  private final boolean readInt96AsFixed;
  private final Set<String> pathsToInt96;

  public AvroSchemaConverterWithTimestampNTZ() {
    this(ADD_LIST_ELEMENT_RECORDS_DEFAULT);
  }

  /**
   * Constructor used by {@link AvroRecordConverter#isElementType}, which always
   * uses the 2-level list conversion.
   *
   * @param assumeRepeatedIsListElement whether to assume 2-level lists
   */
  AvroSchemaConverterWithTimestampNTZ(boolean assumeRepeatedIsListElement) {
    if (!avroVersionSupportsLocalTimestampTypes()) {
      throw new UnsupportedOperationException("Not support local timestamp. Use native avro schema converter");
    }
    this.assumeRepeatedIsListElement = assumeRepeatedIsListElement;
    this.writeOldListStructure = getWriteOldListStructureDefault();
    this.writeParquetUUID = getWriteParquetUUIDDefault();
    this.readInt96AsFixed = getReadInt96AsFixedDefault();
    this.pathsToInt96 = Collections.emptySet();
  }

  public AvroSchemaConverterWithTimestampNTZ(Configuration conf) {
    if (!avroVersionSupportsLocalTimestampTypes()) {
      throw new UnsupportedOperationException("Not support local timestamp. Use native avro schema converter");
    }
    this.assumeRepeatedIsListElement = conf.getBoolean(
        ADD_LIST_ELEMENT_RECORDS, ADD_LIST_ELEMENT_RECORDS_DEFAULT);
    this.writeOldListStructure = conf.getBoolean(
        getWriteOldListStructureConstant(), getWriteOldListStructureDefault());
    this.writeParquetUUID = conf.getBoolean(getWriteParquetUUIDConstant(), getWriteParquetUUIDDefault());
    this.readInt96AsFixed = conf.getBoolean(getReadInt96AsFixedConstant(), getReadInt96AsFixedDefault());
    this.pathsToInt96 = new HashSet<>(Arrays.asList(conf.getStrings("parquet.avro.writeFixedAsInt96", new String[0])));
  }

  /**
   * Given a schema, check to see if it is a union of a null type and a regular schema,
   * and then return the non-null sub-schema. Otherwise, return the given schema.
   *
   * @param schema The schema to check
   * @return The non-null portion of a union schema, or the given schema
   */
  public static Schema getNonNull(Schema schema) {
    if (schema.getType().equals(Schema.Type.UNION)) {
      List<Schema> schemas = schema.getTypes();
      if (schemas.size() == 2) {
        if (schemas.get(0).getType().equals(Schema.Type.NULL)) {
          return schemas.get(1);
        } else if (schemas.get(1).getType().equals(Schema.Type.NULL)) {
          return schemas.get(0);
        } else {
          return schema;
        }
      } else {
        return schema;
      }
    } else {
      return schema;
    }
  }

  @Override
  public MessageType convert(Schema avroSchema) {
    if (!avroSchema.getType().equals(Schema.Type.RECORD)) {
      throw new IllegalArgumentException("Avro schema must be a record.");
    }
    return new MessageType(avroSchema.getFullName(), convertFields(avroSchema.getFields(), ""));
  }

  private List<Type> convertFields(List<Schema.Field> fields, String schemaPath) {
    List<Type> types = new ArrayList<Type>();
    for (Schema.Field field : fields) {
      if (field.schema().getType().equals(Schema.Type.NULL)) {
        continue; // Avro nulls are not encoded, unless they are null unions
      }
      types.add(convertField(field, appendPath(schemaPath, field.name())));
    }
    return types;
  }

  private Type convertField(String fieldName, Schema schema, String schemaPath) {
    return convertField(fieldName, schema, Type.Repetition.REQUIRED, schemaPath);
  }

  @SuppressWarnings("deprecation")
  private Type convertField(String fieldName, Schema schema, Type.Repetition repetition, String schemaPath) {
    Types.PrimitiveBuilder<PrimitiveType> builder;
    Schema.Type type = schema.getType();
    LogicalType logicalType = schema.getLogicalType();
    if (type.equals(Schema.Type.BOOLEAN)) {
      builder = Types.primitive(BOOLEAN, repetition);
    } else if (type.equals(Schema.Type.INT)) {
      builder = Types.primitive(INT32, repetition);
    } else if (type.equals(Schema.Type.LONG)) {
      builder = Types.primitive(INT64, repetition);
    } else if (type.equals(Schema.Type.FLOAT)) {
      builder = Types.primitive(FLOAT, repetition);
    } else if (type.equals(Schema.Type.DOUBLE)) {
      builder = Types.primitive(DOUBLE, repetition);
    } else if (type.equals(Schema.Type.BYTES)) {
      builder = Types.primitive(BINARY, repetition);
    } else if (type.equals(Schema.Type.STRING)) {
      if (logicalType != null && logicalType.getName().equals(LogicalTypes.uuid().getName()) && writeParquetUUID) {
        builder = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
            .length(getUUIDBytes());
      } else {
        builder = Types.primitive(BINARY, repetition);
        Object stringAnnotation = stringType();
        if (stringAnnotation != null) {
          builder = callAsMethod(builder, stringAnnotation);
        }
      }
    } else if (type.equals(Schema.Type.RECORD)) {
      return new GroupType(repetition, fieldName, convertFields(schema.getFields(), schemaPath));
    } else if (type.equals(Schema.Type.ENUM)) {
      Object enumAnnotation = enumType();
      if (enumAnnotation != null) {
        builder = callAsMethod(Types.primitive(BINARY, repetition), enumAnnotation);
      } else {
        builder = Types.primitive(BINARY, repetition);
      }
    } else if (type.equals(Schema.Type.ARRAY)) {
      if (writeOldListStructure) {
        return ConversionPatterns.listType(repetition, fieldName,
            convertField("array", schema.getElementType(), REPEATED, schemaPath));
      } else {
        return ConversionPatterns.listOfElements(repetition, fieldName,
            convertField(AvroWriteSupport.LIST_ELEMENT_NAME, schema.getElementType(), schemaPath));
      }
    } else if (type.equals(Schema.Type.MAP)) {
      Type valType = convertField("value", schema.getValueType(), schemaPath);
      // avro map key type is always string
      return ConversionPatterns.stringKeyMapType(repetition, fieldName, valType);
    } else if (type.equals(Schema.Type.FIXED)) {
      if (pathsToInt96.contains(schemaPath)) {
        if (schema.getFixedSize() != 12) {
          throw new IllegalArgumentException(
              "The size of the fixed type field " + schemaPath + " must be 12 bytes for INT96 conversion");
        }
        builder = Types.primitive(PrimitiveTypeName.INT96, repetition);
      } else {
        builder = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(schema.getFixedSize());
      }
    } else if (type.equals(Schema.Type.UNION)) {
      return convertUnion(fieldName, schema, repetition, schemaPath);
    } else {
      throw new UnsupportedOperationException("Cannot convert Avro type " + type);
    }

    // schema translation can only be done for known logical types because this
    // creates an equivalence
    if (logicalType != null) {
      if (logicalType instanceof LogicalTypes.Decimal) {
        LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
        Object decimalAnnotation = decimalType(decimal.getScale(), decimal.getPrecision());
        if (decimalAnnotation != null) {
          builder = callAsMethod(builder, decimalAnnotation);
        }
      } else {
        Object annotation = convertLogicalType(logicalType);
        if (annotation != null) {
          builder = callAsMethod(builder, annotation);
        }
      }
    }

    return builder.named(fieldName);
  }

  private Type convertUnion(String fieldName, Schema schema, Type.Repetition repetition, String schemaPath) {
    List<Schema> nonNullSchemas = new ArrayList<Schema>(schema.getTypes().size());
    // Found any schemas in the union? Required for the edge case, where the union contains only a single type.
    boolean foundNullSchema = false;
    for (Schema childSchema : schema.getTypes()) {
      if (childSchema.getType().equals(Schema.Type.NULL)) {
        foundNullSchema = true;
        if (Type.Repetition.REQUIRED == repetition) {
          repetition = Type.Repetition.OPTIONAL;
        }
      } else {
        nonNullSchemas.add(childSchema);
      }
    }
    // If we only get a null and one other type then its a simple optional field
    // otherwise construct a union container
    switch (nonNullSchemas.size()) {
      case 0:
        throw new UnsupportedOperationException("Cannot convert Avro union of only nulls");

      case 1:
        return foundNullSchema ? convertField(fieldName, nonNullSchemas.get(0), repetition, schemaPath) :
            convertUnionToGroupType(fieldName, repetition, nonNullSchemas, schemaPath);

      default: // complex union type
        return convertUnionToGroupType(fieldName, repetition, nonNullSchemas, schemaPath);
    }
  }

  private Type convertUnionToGroupType(String fieldName, Type.Repetition repetition, List<Schema> nonNullSchemas,
                                       String schemaPath) {
    List<Type> unionTypes = new ArrayList<Type>(nonNullSchemas.size());
    int index = 0;
    for (Schema childSchema : nonNullSchemas) {
      unionTypes.add( convertField("member" + index++, childSchema, Type.Repetition.OPTIONAL, schemaPath));
    }
    return new GroupType(repetition, fieldName, unionTypes);
  }

  private Type convertField(Schema.Field field, String schemaPath) {
    return convertField(field.name(), field.schema(), schemaPath);
  }

  @Override
  public Schema convert(MessageType parquetSchema) {
    return convertFields(parquetSchema.getName(), parquetSchema.getFields(), new HashMap<>());
  }

  Schema convert(GroupType parquetSchema) {
    return convertFields(parquetSchema.getName(), parquetSchema.getFields(), new HashMap<>());
  }

  private Schema convertFields(String name, List<Type> parquetFields, Map<String, Integer> names) {
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    Integer nameCount = names.merge(name, 1, (oldValue, value) -> oldValue + 1);
    for (Type parquetType : parquetFields) {
      Schema fieldSchema = convertField(parquetType, names);
      if (parquetType.isRepetition(REPEATED)) {
        throw new UnsupportedOperationException("REPEATED not supported outside LIST or MAP. Type: " + parquetType);
      } else if (parquetType.isRepetition(Type.Repetition.OPTIONAL)) {
        fields.add(new Schema.Field(
            parquetType.getName(), optional(fieldSchema), null, NULL_VALUE));
      } else { // REQUIRED
        fields.add(new Schema.Field(
            parquetType.getName(), fieldSchema, null, (Object) null));
      }
    }
    Schema schema = Schema.createRecord(name, null, nameCount > 1 ? name + nameCount : null, false);
    schema.setFields(fields);
    return schema;
  }

  private Schema convertField(final Type parquetType, Map<String, Integer> names) {
    if (parquetType.isPrimitive()) {
      final PrimitiveType asPrimitive = parquetType.asPrimitiveType();
      final PrimitiveTypeName parquetPrimitiveTypeName =
          asPrimitive.getPrimitiveTypeName();
      final Object annotation = getLogicalTypeAnnotation(parquetType);
      Schema schema = parquetPrimitiveTypeName.convert(
          new PrimitiveType.PrimitiveTypeNameConverter<Schema, RuntimeException>() {
            @Override
            public Schema convertBOOLEAN(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.BOOLEAN);
            }
            @Override
            public Schema convertINT32(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.INT);
            }
            @Override
            public Schema convertINT64(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.LONG);
            }
            @Override
            public Schema convertINT96(PrimitiveTypeName primitiveTypeName) {
              if (readInt96AsFixed) {
                return Schema.createFixed("INT96", "INT96 represented as byte[12]", null, 12);
              }
              throw new IllegalArgumentException(
                  "INT96 is deprecated. As interim enable READ_INT96_AS_FIXED flag to read as byte array.");
            }
            @Override
            public Schema convertFLOAT(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.FLOAT);
            }
            @Override
            public Schema convertDOUBLE(PrimitiveTypeName primitiveTypeName) {
              return Schema.create(Schema.Type.DOUBLE);
            }
            @Override
            public Schema convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) {
              if (isInstanceOf(annotation, "org.apache.parquet.schema.LogicalTypeAnnotation$UUIDLogicalTypeAnnotation")) {
                return Schema.create(Schema.Type.STRING);
              } else {
                int size = parquetType.asPrimitiveType().getTypeLength();
                return Schema.createFixed(parquetType.getName(), null, null, size);
              }
            }
            @Override
            public Schema convertBINARY(PrimitiveTypeName primitiveTypeName) {
              if (isInstanceOf(annotation, "org.apache.parquet.schema.LogicalTypeAnnotation$StringLogicalTypeAnnotation") ||
                  isInstanceOf(annotation, "org.apache.parquet.schema.LogicalTypeAnnotation$EnumLogicalTypeAnnotation")) {
                return Schema.create(Schema.Type.STRING);
              } else {
                return Schema.create(Schema.Type.BYTES);
              }
            }
          });

      LogicalType logicalType = convertLogicalType(annotation);
      if (logicalType != null && (!isInstanceOf(annotation, "org.apache.parquet.schema.LogicalTypeAnnotation$DecimalLogicalTypeAnnotation") ||
          parquetPrimitiveTypeName == BINARY ||
          parquetPrimitiveTypeName == FIXED_LEN_BYTE_ARRAY)) {
        schema = logicalType.addToSchema(schema);
      }

      return schema;

    } else {
      GroupType parquetGroupType = parquetType.asGroupType();
      Object logicalTypeAnnotation = getLogicalTypeAnnotation(parquetGroupType);
      if (logicalTypeAnnotation != null) {
        return acceptLogicalTypeAnnotationVisitor(logicalTypeAnnotation, parquetGroupType, names);
      } else {
        // if no original type then it's a record
        return convertFields(parquetGroupType.getName(), parquetGroupType.getFields(), names);
      }
    }
  }

  /**
   * Handles LogicalTypeAnnotation visitor pattern using reflection.
   */
  private Schema acceptLogicalTypeAnnotationVisitor(Object logicalTypeAnnotation, GroupType parquetGroupType, Map<String, Integer> names) {
    if (!isLogicalTypeAnnotationAvailable()) {
      return convertFields(parquetGroupType.getName(), parquetGroupType.getFields(), names);
    }
    try {
      Class<?> visitorClass = Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation$LogicalTypeAnnotationVisitor");
      Class<?> annotationClass = getLogicalTypeAnnotationClass();
      
      // Create visitor implementation using reflection
      Object visitor = java.lang.reflect.Proxy.newProxyInstance(
          visitorClass.getClassLoader(),
          new Class[]{visitorClass},
          (proxy, method, args) -> {
            String methodName = method.getName();
            if ("visit".equals(methodName) && args.length > 0) {
              Object annotation = args[0];
              String annotationClassName = annotation.getClass().getName();
              
              if (annotationClassName.contains("ListLogicalTypeAnnotation")) {
                return visitListLogicalType(parquetGroupType, names);
              } else if (annotationClassName.contains("MapKeyValueTypeAnnotation") || 
                         annotationClassName.contains("MapLogicalTypeAnnotation")) {
                return visitMapLogicalType(parquetGroupType, names);
              } else if (annotationClassName.contains("EnumLogicalTypeAnnotation")) {
                return java.util.Optional.of(Schema.create(Schema.Type.STRING));
              }
            }
            return java.util.Optional.empty();
          });
      
      java.lang.reflect.Method acceptMethod = annotationClass.getMethod("accept", visitorClass);
      @SuppressWarnings("unchecked")
      java.util.Optional<Schema> result = (java.util.Optional<Schema>) acceptMethod.invoke(logicalTypeAnnotation, visitor);
      return result.orElseThrow(() -> new UnsupportedOperationException("Cannot convert Parquet type " + parquetGroupType));
    } catch (Exception e) {
      // Fallback to record conversion
      return convertFields(parquetGroupType.getName(), parquetGroupType.getFields(), names);
    }
  }

  private java.util.Optional<Schema> visitListLogicalType(GroupType parquetGroupType, Map<String, Integer> names) {
    if (parquetGroupType.getFieldCount() != 1) {
      throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
    }
    Type repeatedType = parquetGroupType.getType(0);
    if (!repeatedType.isRepetition(REPEATED)) {
      throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
    }
    if (isElementType(repeatedType, parquetGroupType.getName())) {
      return java.util.Optional.of(Schema.createArray(convertField(repeatedType, names)));
    } else {
      Type elementType = repeatedType.asGroupType().getType(0);
      if (elementType.isRepetition(Type.Repetition.OPTIONAL)) {
        return java.util.Optional.of(Schema.createArray(optional(convertField(elementType, names))));
      } else {
        return java.util.Optional.of(Schema.createArray(convertField(elementType, names)));
      }
    }
  }

  private java.util.Optional<Schema> visitMapLogicalType(GroupType parquetGroupType, Map<String, Integer> names) {
    if (parquetGroupType.getFieldCount() != 1 || parquetGroupType.getType(0).isPrimitive()) {
      throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
    }
    GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
    if (!mapKeyValType.isRepetition(REPEATED) || mapKeyValType.getFieldCount() != 2) {
      throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
    }
    Type keyType = mapKeyValType.getType(0);
    Object keyAnnotation = getLogicalTypeAnnotation(keyType);
    Object stringAnnotation = stringType();
    if (!keyType.isPrimitive() ||
        !keyType.asPrimitiveType().getPrimitiveTypeName().equals(BINARY) ||
        (stringAnnotation != null && !stringAnnotation.equals(keyAnnotation))) {
      throw new IllegalArgumentException("Map key type must be binary (UTF8): " + keyType);
    }
    Type valueType = mapKeyValType.getType(1);
    if (valueType.isRepetition(Type.Repetition.OPTIONAL)) {
      return java.util.Optional.of(Schema.createMap(optional(convertField(valueType, names))));
    } else {
      return java.util.Optional.of(Schema.createMap(convertField(valueType, names)));
    }
  }

  private Object convertLogicalType(LogicalType logicalType) {
    if (logicalType == null) {
      return null;
    } else if (logicalType instanceof LogicalTypes.Decimal) {
      LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
      return decimalType(decimal.getScale(), decimal.getPrecision());
    } else if (logicalType instanceof LogicalTypes.Date) {
      return dateType();
    } else if (logicalType instanceof LogicalTypes.TimeMillis) {
      return timeType(true, getTimeUnitMillis());
    } else if (logicalType instanceof LogicalTypes.TimeMicros) {
      return timeType(true, getTimeUnitMicros());
    } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
      return timestampType(true, getTimeUnitMillis());
    } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
      return timestampType(true, getTimeUnitMicros());
    } else if (logicalType.getName().equals(LogicalTypes.uuid().getName()) && writeParquetUUID) {
      return uuidType();
    }

    if (avroVersionSupportsLocalTimestampTypes()) {
      if (isLocalTimestampMillis(logicalType)) {
        return timestampType(false, getTimeUnitMillis());
      } else if (isLocalTimestampMicros(logicalType)) {
        return timestampType(false, getTimeUnitMicros());
      }
    }

    return null;
  }

  private LogicalType convertLogicalType(Object annotation) {
    if (annotation == null || !isLogicalTypeAnnotationAvailable()) {
      return null;
    }
    try {
      Class<?> visitorClass = Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation$LogicalTypeAnnotationVisitor");
      Class<?> annotationClass = getLogicalTypeAnnotationClass();
      
      // Create visitor implementation using reflection
      Object visitor = java.lang.reflect.Proxy.newProxyInstance(
          visitorClass.getClassLoader(),
          new Class[]{visitorClass},
          (proxy, method, args) -> {
            String methodName = method.getName();
            if ("visit".equals(methodName) && args.length > 0) {
              Object annotationArg = args[0];
              String annotationClassName = annotationArg.getClass().getName();
              
              try {
                if (annotationClassName.contains("DecimalLogicalTypeAnnotation")) {
                  java.lang.reflect.Method getPrecision = annotationArg.getClass().getMethod("getPrecision");
                  java.lang.reflect.Method getScale = annotationArg.getClass().getMethod("getScale");
                  int precision = (Integer) getPrecision.invoke(annotationArg);
                  int scale = (Integer) getScale.invoke(annotationArg);
                  return java.util.Optional.of(LogicalTypes.decimal(precision, scale));
                } else if (annotationClassName.contains("DateLogicalTypeAnnotation")) {
                  return java.util.Optional.of(LogicalTypes.date());
                } else if (annotationClassName.contains("TimeLogicalTypeAnnotation")) {
                  java.lang.reflect.Method getUnit = annotationArg.getClass().getMethod("getUnit");
                  Object unit = getUnit.invoke(annotationArg);
                  String unitName = unit.toString();
                  if ("MILLIS".equals(unitName)) {
                    return java.util.Optional.of(LogicalTypes.timeMillis());
                  } else if ("MICROS".equals(unitName)) {
                    return java.util.Optional.of(LogicalTypes.timeMicros());
                  }
                } else if (annotationClassName.contains("TimestampLogicalTypeAnnotation")) {
                  java.lang.reflect.Method getUnit = annotationArg.getClass().getMethod("getUnit");
                  java.lang.reflect.Method isAdjustedToUTC = annotationArg.getClass().getMethod("isAdjustedToUTC");
                  Object unit = getUnit.invoke(annotationArg);
                  boolean adjusted = (Boolean) isAdjustedToUTC.invoke(annotationArg);
                  String unitName = unit.toString();
                  
                  if (adjusted || !avroVersionSupportsLocalTimestampTypes()) {
                    if ("MILLIS".equals(unitName)) {
                      return java.util.Optional.of(LogicalTypes.timestampMillis());
                    } else if ("MICROS".equals(unitName)) {
                      return java.util.Optional.of(LogicalTypes.timestampMicros());
                    }
                  } else {
                    if ("MILLIS".equals(unitName)) {
                      LogicalType localTimestampMillis = createLocalTimestampMillis();
                      if (localTimestampMillis != null) {
                        return java.util.Optional.of(localTimestampMillis);
                      }
                    } else if ("MICROS".equals(unitName)) {
                      LogicalType localTimestampMicros = createLocalTimestampMicros();
                      if (localTimestampMicros != null) {
                        return java.util.Optional.of(localTimestampMicros);
                      }
                    }
                  }
                } else if (annotationClassName.contains("UUIDLogicalTypeAnnotation")) {
                  return java.util.Optional.of(LogicalTypes.uuid());
                }
              } catch (Exception e) {
                // Ignore and return empty
              }
            }
            return java.util.Optional.empty();
          });
      
      java.lang.reflect.Method acceptMethod = annotationClass.getMethod("accept", visitorClass);
      @SuppressWarnings("unchecked")
      java.util.Optional<LogicalType> result = (java.util.Optional<LogicalType>) acceptMethod.invoke(annotation, visitor);
      return result.orElse(null);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Implements the rules for interpreting existing data from the logical type
   * spec for the LIST annotation. This is used to produce the expected schema.
   * <p>
   * The AvroArrayConverter will decide whether the repeated type is the array
   * element type by testing whether the element schema and repeated type are
   * the same. This ensures that the LIST rules are followed when there is no
   * schema and that a schema can be provided to override the default behavior.
   */
  private boolean isElementType(Type repeatedType, String parentName) {
    return (
        // can't be a synthetic layer because it would be invalid
        repeatedType.isPrimitive() ||
            repeatedType.asGroupType().getFieldCount() > 1 ||
            repeatedType.asGroupType().getType(0).isRepetition(REPEATED) ||
            // known patterns without the synthetic layer
            repeatedType.getName().equals("array") ||
            repeatedType.getName().equals(parentName + "_tuple") ||
            // default assumption
            assumeRepeatedIsListElement
    );
  }

  private static Schema optional(Schema original) {
    // null is first in the union because Parquet's default is always null
    return Schema.createUnion(Arrays.asList(
        Schema.create(Schema.Type.NULL),
        original));
  }

  private static String appendPath(String path, String fieldName) {
    if (path == null || path.isEmpty()) {
      return fieldName;
    }
    return path + '.' + fieldName;
  }

  /* Avro <= 1.9 does not support conversions to LocalTimestamp{Micros, Millis} classes */
  private static boolean avroVersionSupportsLocalTimestampTypes() {
    final String avroVersion = getRuntimeAvroVersion();

    return avroVersion == null
        || !(avroVersion.startsWith("1.7.")
        || avroVersion.startsWith("1.8.")
        || avroVersion.startsWith("1.9.")
        || avroVersion.startsWith("1.10.")
        || avroVersion.startsWith("1.11.")
        || avroVersion.startsWith("1.12.")
        || avroVersion.startsWith("1.13.")
        || avroVersion.startsWith("1.14."));
  }

  private static String getRuntimeAvroVersion() {
    return Schema.Parser.class.getPackage().getImplementationVersion();
  }

  /**
   * Checks if a logical type is an instance of LocalTimestampMillis using reflection.
   * Returns false if the class doesn't exist (e.g., in Avro 1.8.2).
   */
  private static boolean isLocalTimestampMillis(LogicalType logicalType) {
    if (logicalType == null) {
      return false;
    }
    try {
      Class<?> localTimestampMillisClass = Class.forName("org.apache.avro.LogicalTypes$LocalTimestampMillis");
      return localTimestampMillisClass.isInstance(logicalType);
    } catch (ClassNotFoundException e) {
      // Class doesn't exist (e.g., Avro 1.8.2)
      return false;
    }
  }

  /**
   * Checks if a logical type is an instance of LocalTimestampMicros using reflection.
   * Returns false if the class doesn't exist (e.g., in Avro 1.8.2).
   */
  private static boolean isLocalTimestampMicros(LogicalType logicalType) {
    if (logicalType == null) {
      return false;
    }
    try {
      Class<?> localTimestampMicrosClass = Class.forName("org.apache.avro.LogicalTypes$LocalTimestampMicros");
      return localTimestampMicrosClass.isInstance(logicalType);
    } catch (ClassNotFoundException e) {
      // Class doesn't exist (e.g., Avro 1.8.2)
      return false;
    }
  }

  /**
   * Creates a LocalTimestampMillis logical type using reflection.
   * Returns null if the method doesn't exist (e.g., in Avro 1.8.2).
   */
  private static LogicalType createLocalTimestampMillis() {
    try {
      java.lang.reflect.Method method = LogicalTypes.class.getMethod("localTimestampMillis");
      return (LogicalType) method.invoke(null);
    } catch (Exception e) {
      // Method doesn't exist (e.g., Avro 1.8.2)
      return null;
    }
  }

  /**
   * Creates a LocalTimestampMicros logical type using reflection.
   * Returns null if the method doesn't exist (e.g., in Avro 1.8.2).
   */
  private static LogicalType createLocalTimestampMicros() {
    try {
      java.lang.reflect.Method method = LogicalTypes.class.getMethod("localTimestampMicros");
      return (LogicalType) method.invoke(null);
    } catch (Exception e) {
      // Method doesn't exist (e.g., Avro 1.8.2)
      return null;
    }
  }

  // ========== LogicalTypeAnnotation reflection helpers ==========
  // These methods use reflection to access LogicalTypeAnnotation which may not
  // be available in older Parquet versions (e.g., used by Spark 2.4)

  /**
   * Checks if LogicalTypeAnnotation class is available.
   */
  private static boolean isLogicalTypeAnnotationAvailable() {
    try {
      Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation");
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Gets the LogicalTypeAnnotation class using reflection.
   */
  private static Class<?> getLogicalTypeAnnotationClass() {
    try {
      return Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("LogicalTypeAnnotation is not available in this Parquet version", e);
    }
  }

  /**
   * Gets LogicalTypeAnnotation from a Type using reflection.
   */
  private static Object getLogicalTypeAnnotation(Type type) {
    if (!isLogicalTypeAnnotationAvailable()) {
      return null;
    }
    try {
      java.lang.reflect.Method method = type.getClass().getMethod("getLogicalTypeAnnotation");
      return method.invoke(type);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Creates a dateType() using reflection.
   */
  private static Object dateType() {
    if (!isLogicalTypeAnnotationAvailable()) {
      return null;
    }
    try {
      java.lang.reflect.Method method = getLogicalTypeAnnotationClass().getMethod("dateType");
      return method.invoke(null);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Creates a decimalType(scale, precision) using reflection.
   */
  private static Object decimalType(int scale, int precision) {
    if (!isLogicalTypeAnnotationAvailable()) {
      return null;
    }
    try {
      java.lang.reflect.Method method = getLogicalTypeAnnotationClass().getMethod("decimalType", int.class, int.class);
      return method.invoke(null, scale, precision);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Creates an enumType() using reflection.
   */
  private static Object enumType() {
    if (!isLogicalTypeAnnotationAvailable()) {
      return null;
    }
    try {
      java.lang.reflect.Method method = getLogicalTypeAnnotationClass().getMethod("enumType");
      return method.invoke(null);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Creates a stringType() using reflection.
   */
  private static Object stringType() {
    if (!isLogicalTypeAnnotationAvailable()) {
      return null;
    }
    try {
      java.lang.reflect.Method method = getLogicalTypeAnnotationClass().getMethod("stringType");
      return method.invoke(null);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Creates a timeType(isAdjustedToUTC, unit) using reflection.
   */
  private static Object timeType(boolean isAdjustedToUTC, Object unit) {
    if (!isLogicalTypeAnnotationAvailable()) {
      return null;
    }
    try {
      Class<?> timeUnitClass = Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation$TimeUnit");
      java.lang.reflect.Method method = getLogicalTypeAnnotationClass().getMethod("timeType", boolean.class, timeUnitClass);
      return method.invoke(null, isAdjustedToUTC, unit);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Creates a timestampType(isAdjustedToUTC, unit) using reflection.
   */
  private static Object timestampType(boolean isAdjustedToUTC, Object unit) {
    if (!isLogicalTypeAnnotationAvailable()) {
      return null;
    }
    try {
      Class<?> timeUnitClass = Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation$TimeUnit");
      java.lang.reflect.Method method = getLogicalTypeAnnotationClass().getMethod("timestampType", boolean.class, timeUnitClass);
      return method.invoke(null, isAdjustedToUTC, unit);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Creates a uuidType() using reflection.
   */
  private static Object uuidType() {
    if (!isLogicalTypeAnnotationAvailable()) {
      return null;
    }
    try {
      java.lang.reflect.Method method = getLogicalTypeAnnotationClass().getMethod("uuidType");
      return method.invoke(null);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Gets TimeUnit.MICROS enum value using reflection.
   */
  private static Object getTimeUnitMicros() {
    try {
      Class<?> timeUnitClass = Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation$TimeUnit");
      return Enum.valueOf((Class<Enum>) timeUnitClass, "MICROS");
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Gets TimeUnit.MILLIS enum value using reflection.
   */
  private static Object getTimeUnitMillis() {
    try {
      Class<?> timeUnitClass = Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation$TimeUnit");
      return Enum.valueOf((Class<Enum>) timeUnitClass, "MILLIS");
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Gets UUIDLogicalTypeAnnotation.BYTES constant using reflection.
   */
  private static int getUUIDBytes() {
    try {
      Class<?> uuidClass = Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation$UUIDLogicalTypeAnnotation");
      java.lang.reflect.Field field = uuidClass.getField("BYTES");
      return field.getInt(null);
    } catch (Exception e) {
      return 16; // Default UUID size
    }
  }

  /**
   * Checks if an annotation is an instance of a specific LogicalTypeAnnotation subclass using reflection.
   */
  private static boolean isInstanceOf(Object annotation, String className) {
    if (annotation == null) {
      return false;
    }
    try {
      Class<?> clazz = Class.forName(className);
      return clazz.isInstance(annotation);
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Calls the as() method on a builder using reflection to avoid compile-time dependency on LogicalTypeAnnotation.
   */
  @SuppressWarnings("unchecked")
  private static <T> T callAsMethod(T builder, Object annotation) {
    if (annotation == null) {
      return builder;
    }
    try {
      java.lang.reflect.Method asMethod = builder.getClass().getMethod("as", Object.class);
      return (T) asMethod.invoke(builder, annotation);
    } catch (Exception e) {
      // If as() method doesn't exist or fails, try with LogicalTypeAnnotation type
      try {
        Class<?> annotationClass = Class.forName("org.apache.parquet.schema.LogicalTypeAnnotation");
        java.lang.reflect.Method asMethod = builder.getClass().getMethod("as", annotationClass);
        return (T) asMethod.invoke(builder, annotation);
      } catch (Exception e2) {
        // Fallback: return builder as-is
        return builder;
      }
    }
  }

  // ========== AvroWriteSupport and AvroReadSupport constants reflection helpers ==========
  // These methods use reflection to access constants which may not be available in older Parquet versions

  /**
   * Gets WRITE_OLD_LIST_STRUCTURE constant using reflection.
   */
  private static String getWriteOldListStructureConstant() {
    try {
      Class<?> clazz = Class.forName("org.apache.parquet.avro.AvroWriteSupport");
      java.lang.reflect.Field field = clazz.getField("WRITE_OLD_LIST_STRUCTURE");
      return (String) field.get(null);
    } catch (Exception e) {
      return "parquet.avro.writeOldListStructure";
    }
  }

  /**
   * Gets WRITE_OLD_LIST_STRUCTURE_DEFAULT constant using reflection.
   */
  private static boolean getWriteOldListStructureDefault() {
    try {
      Class<?> clazz = Class.forName("org.apache.parquet.avro.AvroWriteSupport");
      java.lang.reflect.Field field = clazz.getField("WRITE_OLD_LIST_STRUCTURE_DEFAULT");
      return field.getBoolean(null);
    } catch (Exception e) {
      return false; // Default value
    }
  }

  /**
   * Gets WRITE_PARQUET_UUID constant using reflection.
   */
  private static String getWriteParquetUUIDConstant() {
    try {
      Class<?> clazz = Class.forName("org.apache.parquet.avro.AvroWriteSupport");
      java.lang.reflect.Field field = clazz.getField("WRITE_PARQUET_UUID");
      return (String) field.get(null);
    } catch (Exception e) {
      return "parquet.avro.writeParquetUUID";
    }
  }

  /**
   * Gets WRITE_PARQUET_UUID_DEFAULT constant using reflection.
   */
  private static boolean getWriteParquetUUIDDefault() {
    try {
      Class<?> clazz = Class.forName("org.apache.parquet.avro.AvroWriteSupport");
      java.lang.reflect.Field field = clazz.getField("WRITE_PARQUET_UUID_DEFAULT");
      return field.getBoolean(null);
    } catch (Exception e) {
      return false; // Default value
    }
  }

  /**
   * Gets READ_INT96_AS_FIXED constant using reflection.
   */
  private static String getReadInt96AsFixedConstant() {
    try {
      Class<?> clazz = Class.forName("org.apache.parquet.avro.AvroReadSupport");
      java.lang.reflect.Field field = clazz.getField("READ_INT96_AS_FIXED");
      return (String) field.get(null);
    } catch (Exception e) {
      return "parquet.avro.readInt96AsFixed";
    }
  }

  /**
   * Gets READ_INT96_AS_FIXED_DEFAULT constant using reflection.
   */
  private static boolean getReadInt96AsFixedDefault() {
    try {
      Class<?> clazz = Class.forName("org.apache.parquet.avro.AvroReadSupport");
      java.lang.reflect.Field field = clazz.getField("READ_INT96_AS_FIXED_DEFAULT");
      return field.getBoolean(null);
    } catch (Exception e) {
      return false; // Default value
    }
  }
}
