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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
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
import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED;
import static org.apache.parquet.avro.AvroReadSupport.READ_INT96_AS_FIXED_DEFAULT;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE_DEFAULT;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_PARQUET_UUID;
import static org.apache.parquet.avro.AvroWriteSupport.WRITE_PARQUET_UUID_DEFAULT;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MILLIS;
import static org.apache.parquet.schema.LogicalTypeAnnotation.dateType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.decimalType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.enumType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timeType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.timestampType;
import static org.apache.parquet.schema.LogicalTypeAnnotation.uuidType;
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
public class AvroSchemaConverterWithNTZ extends HoodieAvroParquetSchemaConverter {

  public static final String ADD_LIST_ELEMENT_RECORDS =
      "parquet.avro.add-list-element-records";
  private static final boolean ADD_LIST_ELEMENT_RECORDS_DEFAULT = true;

  private final boolean assumeRepeatedIsListElement;
  private final boolean writeOldListStructure;
  private final boolean writeParquetUUID;
  private final boolean readInt96AsFixed;
  private final Set<String> pathsToInt96;

  public AvroSchemaConverterWithNTZ() {
    this(ADD_LIST_ELEMENT_RECORDS_DEFAULT);
  }

  /**
   * Constructor used by {@link AvroRecordConverter#isElementType}, which always
   * uses the 2-level list conversion.
   *
   * @param assumeRepeatedIsListElement whether to assume 2-level lists
   */
  AvroSchemaConverterWithNTZ(boolean assumeRepeatedIsListElement) {
    this.assumeRepeatedIsListElement = assumeRepeatedIsListElement;
    this.writeOldListStructure = WRITE_OLD_LIST_STRUCTURE_DEFAULT;
    this.writeParquetUUID = WRITE_PARQUET_UUID_DEFAULT;
    this.readInt96AsFixed = READ_INT96_AS_FIXED_DEFAULT;
    this.pathsToInt96 = Collections.emptySet();
  }

  public AvroSchemaConverterWithNTZ(Configuration conf) {
    this.assumeRepeatedIsListElement = conf.getBoolean(
        ADD_LIST_ELEMENT_RECORDS, ADD_LIST_ELEMENT_RECORDS_DEFAULT);
    this.writeOldListStructure = conf.getBoolean(
        WRITE_OLD_LIST_STRUCTURE, WRITE_OLD_LIST_STRUCTURE_DEFAULT);
    this.writeParquetUUID = conf.getBoolean(WRITE_PARQUET_UUID, WRITE_PARQUET_UUID_DEFAULT);
    this.readInt96AsFixed = conf.getBoolean(READ_INT96_AS_FIXED, READ_INT96_AS_FIXED_DEFAULT);
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
            .length(LogicalTypeAnnotation.UUIDLogicalTypeAnnotation.BYTES);
      } else {
        builder = Types.primitive(BINARY, repetition).as(stringType());
      }
    } else if (type.equals(Schema.Type.RECORD)) {
      return new GroupType(repetition, fieldName, convertFields(schema.getFields(), schemaPath));
    } else if (type.equals(Schema.Type.ENUM)) {
      builder = Types.primitive(BINARY, repetition).as(enumType());
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
        builder = builder.as(decimalType(decimal.getScale(), decimal.getPrecision()));
      } else {
        LogicalTypeAnnotation annotation = convertLogicalType(logicalType);
        if (annotation != null) {
          builder.as(annotation);
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
      final LogicalTypeAnnotation annotation = parquetType.getLogicalTypeAnnotation();
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
              if (annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
                return Schema.create(Schema.Type.STRING);
              } else {
                int size = parquetType.asPrimitiveType().getTypeLength();
                return Schema.createFixed(parquetType.getName(), null, null, size);
              }
            }
            @Override
            public Schema convertBINARY(PrimitiveTypeName primitiveTypeName) {
              if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation ||
                  annotation instanceof  LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
                return Schema.create(Schema.Type.STRING);
              } else {
                return Schema.create(Schema.Type.BYTES);
              }
            }
          });

      LogicalType logicalType = convertLogicalType(annotation);
      if (logicalType != null && (!(annotation instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) ||
          parquetPrimitiveTypeName == BINARY ||
          parquetPrimitiveTypeName == FIXED_LEN_BYTE_ARRAY)) {
        schema = logicalType.addToSchema(schema);
      }

      return schema;

    } else {
      GroupType parquetGroupType = parquetType.asGroupType();
      LogicalTypeAnnotation logicalTypeAnnotation = parquetGroupType.getLogicalTypeAnnotation();
      if (logicalTypeAnnotation != null) {
        return logicalTypeAnnotation.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Schema>() {
          @Override
          public java.util.Optional<Schema> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
            if (parquetGroupType.getFieldCount()!= 1) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            Type repeatedType = parquetGroupType.getType(0);
            if (!repeatedType.isRepetition(REPEATED)) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            if (isElementType(repeatedType, parquetGroupType.getName())) {
              // repeated element types are always required
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

          @Override
          // for backward-compatibility
          public java.util.Optional<Schema> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
            return visitMapOrMapKeyValue();
          }

          @Override
          public java.util.Optional<Schema> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
            return visitMapOrMapKeyValue();
          }

          private java.util.Optional<Schema> visitMapOrMapKeyValue() {
            if (parquetGroupType.getFieldCount() != 1 || parquetGroupType.getType(0).isPrimitive()) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
            if (!mapKeyValType.isRepetition(REPEATED) ||
                mapKeyValType.getFieldCount()!=2) {
              throw new UnsupportedOperationException("Invalid map type " + parquetGroupType);
            }
            Type keyType = mapKeyValType.getType(0);
            if (!keyType.isPrimitive() ||
                !keyType.asPrimitiveType().getPrimitiveTypeName().equals(BINARY) ||
                !keyType.getLogicalTypeAnnotation().equals(stringType())) {
              throw new IllegalArgumentException("Map key type must be binary (UTF8): "
                  + keyType);
            }
            Type valueType = mapKeyValType.getType(1);
            if (valueType.isRepetition(Type.Repetition.OPTIONAL)) {
              return java.util.Optional.of(Schema.createMap(optional(convertField(valueType, names))));
            } else {
              return java.util.Optional.of(Schema.createMap(convertField(valueType, names)));
            }
          }

          @Override
          public java.util.Optional<Schema> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
            return java.util.Optional.of(Schema.create(Schema.Type.STRING));
          }
        }).orElseThrow(() -> new UnsupportedOperationException("Cannot convert Parquet type " + parquetType));
      } else {
        // if no original type then it's a record
        return convertFields(parquetGroupType.getName(), parquetGroupType.getFields(), names);
      }
    }
  }

  private LogicalTypeAnnotation convertLogicalType(LogicalType logicalType) {
    if (logicalType == null) {
      return null;
    } else if (logicalType instanceof LogicalTypes.Decimal) {
      LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
      return decimalType(decimal.getScale(), decimal.getPrecision());
    } else if (logicalType instanceof LogicalTypes.Date) {
      return dateType();
    } else if (logicalType instanceof LogicalTypes.TimeMillis) {
      return timeType(true, MILLIS);
    } else if (logicalType instanceof LogicalTypes.TimeMicros) {
      return timeType(true, MICROS);
    } else if (logicalType instanceof LogicalTypes.TimestampMillis) {
      return timestampType(true, MILLIS);
    } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
      return timestampType(true, MICROS);
    } else if (logicalType.getName().equals(LogicalTypes.uuid().getName()) && writeParquetUUID) {
      return uuidType();
    }

    if (avroVersionSupportsLocalTimestampTypes()) {
      if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
        return timestampType(false, MILLIS);
      } else if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
        return timestampType(false, MICROS);
      }
    }

    return null;
  }

  private LogicalType convertLogicalType(LogicalTypeAnnotation annotation) {
    if (annotation == null) {
      return null;
    }
    return annotation
        .accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<LogicalType>() {
          @Override
          public java.util.Optional<LogicalType> visit(
              LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
            return java.util.Optional.of(
                LogicalTypes.decimal(decimalLogicalType.getPrecision(), decimalLogicalType.getScale()));
          }

          @Override
          public java.util.Optional<LogicalType> visit(
              LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
            return java.util.Optional.of(LogicalTypes.date());
          }

          @Override
          public java.util.Optional<LogicalType> visit(
              LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
            LogicalTypeAnnotation.TimeUnit unit = timeLogicalType.getUnit();
            switch (unit) {
              case MILLIS:
                return java.util.Optional.of(LogicalTypes.timeMillis());
              case MICROS:
                return java.util.Optional.of(LogicalTypes.timeMicros());
            }
            return java.util.Optional.empty();
          }

          @Override
          public java.util.Optional<LogicalType> visit(
              LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
            LogicalTypeAnnotation.TimeUnit unit = timestampLogicalType.getUnit();
            boolean isAdjustedToUTC = timestampLogicalType.isAdjustedToUTC();

            if (isAdjustedToUTC || !avroVersionSupportsLocalTimestampTypes()) {
              switch (unit) {
                case MILLIS:
                  return java.util.Optional.of(LogicalTypes.timestampMillis());
                case MICROS:
                  return java.util.Optional.of(LogicalTypes.timestampMicros());
              }
              return java.util.Optional.empty();
            } else {
              switch (unit) {
                case MILLIS:
                  return java.util.Optional.of(LogicalTypes.localTimestampMillis());
                case MICROS:
                  return java.util.Optional.of(LogicalTypes.localTimestampMicros());
              }
              return java.util.Optional.empty();
            }
          }

          @Override
          public java.util.Optional<LogicalType> visit(UUIDLogicalTypeAnnotation uuidLogicalType) {
            return java.util.Optional.of(LogicalTypes.uuid());
          }
        })
        .orElse(null);
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
        || avroVersion.startsWith("1.9."));
  }

  private static String getRuntimeAvroVersion() {
    return Schema.Parser.class.getPackage().getImplementationVersion();
  }
}
