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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

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
    this.assumeRepeatedIsListElement = assumeRepeatedIsListElement;
    this.writeOldListStructure = WRITE_OLD_LIST_STRUCTURE_DEFAULT;
    this.writeParquetUUID = WRITE_PARQUET_UUID_DEFAULT;
    this.readInt96AsFixed = READ_INT96_AS_FIXED_DEFAULT;
    this.pathsToInt96 = Collections.emptySet();
  }

  public AvroSchemaConverterWithTimestampNTZ(Configuration conf) {
    this.assumeRepeatedIsListElement = conf.getBoolean(
        ADD_LIST_ELEMENT_RECORDS, ADD_LIST_ELEMENT_RECORDS_DEFAULT);
    this.writeOldListStructure = conf.getBoolean(
        WRITE_OLD_LIST_STRUCTURE, WRITE_OLD_LIST_STRUCTURE_DEFAULT);
    this.writeParquetUUID = conf.getBoolean(WRITE_PARQUET_UUID, WRITE_PARQUET_UUID_DEFAULT);
    this.readInt96AsFixed = conf.getBoolean(READ_INT96_AS_FIXED, READ_INT96_AS_FIXED_DEFAULT);
    this.pathsToInt96 = new HashSet<>(Arrays.asList(conf.getStrings("parquet.avro.writeFixedAsInt96", new String[0])));
  }

  @Override
  public MessageType convert(HoodieSchema schema) {
    if (schema.getType() != HoodieSchemaType.RECORD) {
      throw new IllegalArgumentException("Hoodie schema must be a record.");
    }
    return new MessageType(schema.getFullName(), convertFields(schema.getFields(), ""));
  }

  private List<Type> convertFields(List<HoodieSchemaField> fields, String schemaPath) {
    List<Type> types = new ArrayList<Type>(fields.size());
    for (HoodieSchemaField field : fields) {
      if (field.schema().getType() == HoodieSchemaType.NULL) {
        continue; // Nulls are not encoded, unless they are null unions
      }
      types.add(convertField(field, appendPath(schemaPath, field.name())));
    }
    return types;
  }

  private Type convertField(String fieldName, HoodieSchema schema, String schemaPath) {
    return convertField(fieldName, schema, Type.Repetition.REQUIRED, schemaPath);
  }

  @SuppressWarnings("deprecation")
  private Type convertField(String fieldName, HoodieSchema schema, Type.Repetition repetition, String schemaPath) {
    Types.PrimitiveBuilder<PrimitiveType> builder;
    HoodieSchemaType type = schema.getType();
    switch (type) {
      case BOOLEAN:
        builder = Types.primitive(BOOLEAN, repetition);
        break;
      case INT:
        builder = Types.primitive(INT32, repetition);
        break;
      case LONG:
        builder = Types.primitive(INT64, repetition);
        break;
      case FLOAT:
        builder = Types.primitive(FLOAT, repetition);
        break;
      case DOUBLE:
        builder = Types.primitive(DOUBLE, repetition);
        break;
      case BYTES:
        builder = Types.primitive(BINARY, repetition);
        break;
      case STRING:
        builder = Types.primitive(BINARY, repetition).as(stringType());
        break;
      case UUID:
        if (writeParquetUUID) {
          builder = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
              .length(LogicalTypeAnnotation.UUIDLogicalTypeAnnotation.BYTES)
              .as(uuidType());
        } else {
          builder = Types.primitive(BINARY, repetition).as(stringType());
        }
        break;
      case DECIMAL:
        HoodieSchema.Decimal decimalSchema = (HoodieSchema.Decimal) schema;
        if (decimalSchema.isFixed()) {
          builder = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
              .length(decimalSchema.getFixedSize())
              .as(decimalType(decimalSchema.getScale(), decimalSchema.getPrecision()));
        } else {
          builder = Types.primitive(BINARY, repetition)
              .as(decimalType(decimalSchema.getScale(), decimalSchema.getPrecision()));
        }
        break;
      case DATE:
        builder = Types.primitive(INT32, repetition).as(dateType());
        break;
      case TIME:
        HoodieSchema.Time timeSchema = (HoodieSchema.Time) schema;
        switch (timeSchema.getPrecision()) {
          case MILLIS:
            builder = Types.primitive(INT32, repetition)
                .as(timeType(true, MILLIS));
            break;
          case MICROS:
            builder = Types.primitive(INT64, repetition)
                .as(timeType(true, MICROS));
            break;
          default:
            throw new IllegalArgumentException("Unsupported precision: " + timeSchema.getPrecision());
        }
        break;
      case TIMESTAMP:
        HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) schema;
        switch (timestampSchema.getPrecision()) {
          case MILLIS:
            builder = Types.primitive(INT64, repetition)
                .as(timestampType(timestampSchema.isUtcAdjusted(), MILLIS));
            break;
          case MICROS:
            builder = Types.primitive(INT64, repetition)
                .as(timestampType(timestampSchema.isUtcAdjusted(), MICROS));
            break;
          default:
            throw new IllegalArgumentException("Unsupported precision: " + timestampSchema.getPrecision());
        }
        break;
      case RECORD:
        return new GroupType(repetition, fieldName, convertFields(schema.getFields(), schemaPath));
      case ENUM:
        builder = Types.primitive(BINARY, repetition).as(enumType());
        break;
      case ARRAY:
        if (writeOldListStructure) {
          return ConversionPatterns.listType(repetition, fieldName,
              convertField("array", schema.getElementType(), REPEATED, schemaPath));
        } else {
          return ConversionPatterns.listOfElements(repetition, fieldName,
              convertField(AvroWriteSupport.LIST_ELEMENT_NAME, schema.getElementType(), schemaPath));
        }
      case MAP:
        Type valType = convertField("value", schema.getValueType(), schemaPath);
        // avro map key type is always string
        return ConversionPatterns.stringKeyMapType(repetition, fieldName, valType);
      case FIXED:
        if (pathsToInt96.contains(schemaPath)) {
          if (schema.getFixedSize() != 12) {
            throw new IllegalArgumentException(
                "The size of the fixed type field " + schemaPath + " must be 12 bytes for INT96 conversion");
          }
          builder = Types.primitive(PrimitiveTypeName.INT96, repetition);
        } else {
          builder = Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition).length(schema.getFixedSize());
        }
        break;
      case UNION:
        return convertUnion(fieldName, schema, repetition, schemaPath);
      default:
        throw new UnsupportedOperationException("Cannot convert Avro type " + type);
    }
    return builder.named(fieldName);
  }

  private Type convertUnion(String fieldName, HoodieSchema schema, Type.Repetition repetition, String schemaPath) {
    List<HoodieSchema> nonNullSchemas = new ArrayList<>(schema.getTypes().size());
    // Found any schemas in the union? Required for the edge case, where the union contains only a single type.
    boolean foundNullSchema = false;
    for (HoodieSchema childSchema : schema.getTypes()) {
      if (childSchema.getType() == HoodieSchemaType.NULL) {
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

  private Type convertUnionToGroupType(String fieldName, Type.Repetition repetition, List<HoodieSchema> nonNullSchemas,
                                       String schemaPath) {
    List<Type> unionTypes = new ArrayList<Type>(nonNullSchemas.size());
    int index = 0;
    for (HoodieSchema childSchema : nonNullSchemas) {
      unionTypes.add( convertField("member" + index++, childSchema, Type.Repetition.OPTIONAL, schemaPath));
    }
    return new GroupType(repetition, fieldName, unionTypes);
  }

  private Type convertField(HoodieSchemaField field, String schemaPath) {
    return convertField(field.name(), field.schema(), schemaPath);
  }

  @Override
  public HoodieSchema convert(MessageType parquetSchema) {
    return convertFields(parquetSchema.getName(), parquetSchema.getFields(), new HashMap<>());
  }

  HoodieSchema convert(GroupType parquetSchema) {
    return convertFields(parquetSchema.getName(), parquetSchema.getFields(), new HashMap<>());
  }

  private HoodieSchema convertFields(String name, List<Type> parquetFields, Map<String, Integer> names) {
    List<HoodieSchemaField> fields = new ArrayList<>(parquetFields.size());
    Integer nameCount = names.merge(name, 1, (oldValue, value) -> oldValue + 1);
    for (Type parquetType : parquetFields) {
      HoodieSchema fieldSchema = convertField(parquetType, names);
      if (parquetType.isRepetition(REPEATED)) {
        throw new UnsupportedOperationException("REPEATED not supported outside LIST or MAP. Type: " + parquetType);
      } else if (parquetType.isRepetition(Type.Repetition.OPTIONAL)) {
        fields.add(HoodieSchemaField.of(
            parquetType.getName(), optional(fieldSchema), null, NULL_VALUE));
      } else { // REQUIRED
        fields.add(HoodieSchemaField.of(
            parquetType.getName(), fieldSchema, null, (Object) null));
      }
    }
    HoodieSchema schema = HoodieSchema.createRecord(name, null, nameCount > 1 ? name + nameCount : null, false, fields);
    return schema;
  }

  private HoodieSchema convertField(final Type parquetType, Map<String, Integer> names) {
    if (parquetType.isPrimitive()) {
      final PrimitiveType asPrimitive = parquetType.asPrimitiveType();
      final PrimitiveTypeName parquetPrimitiveTypeName =
          asPrimitive.getPrimitiveTypeName();
      final LogicalTypeAnnotation annotation = parquetType.getLogicalTypeAnnotation();

      // Handle logical type annotations directly with HoodieSchema creation methods
      if (annotation != null) {
        HoodieSchema logicalSchema = convertLogicalTypeAnnotationToHoodieSchema(annotation, parquetType);
        if (logicalSchema != null) {
          return logicalSchema;
        }
      }

      // Fallback to basic type conversion if no logical type annotation
      HoodieSchema schema = parquetPrimitiveTypeName.convert(
          new PrimitiveType.PrimitiveTypeNameConverter<HoodieSchema, RuntimeException>() {
            @Override
            public HoodieSchema convertBOOLEAN(PrimitiveTypeName primitiveTypeName) {
              return HoodieSchema.create(HoodieSchemaType.BOOLEAN);
            }
            @Override
            public HoodieSchema convertINT32(PrimitiveTypeName primitiveTypeName) {
              return HoodieSchema.create(HoodieSchemaType.INT);
            }
            @Override
            public HoodieSchema convertINT64(PrimitiveTypeName primitiveTypeName) {
              return HoodieSchema.create(HoodieSchemaType.LONG);
            }
            @Override
            public HoodieSchema convertINT96(PrimitiveTypeName primitiveTypeName) {
              if (readInt96AsFixed) {
                return HoodieSchema.createFixed("INT96", "INT96 represented as byte[12]", null, 12);
              }
              throw new IllegalArgumentException(
                  "INT96 is deprecated. As interim enable READ_INT96_AS_FIXED flag to read as byte array.");
            }
            @Override
            public HoodieSchema convertFLOAT(PrimitiveTypeName primitiveTypeName) {
              return HoodieSchema.create(HoodieSchemaType.FLOAT);
            }
            @Override
            public HoodieSchema convertDOUBLE(PrimitiveTypeName primitiveTypeName) {
              return HoodieSchema.create(HoodieSchemaType.DOUBLE);
            }
            @Override
            public HoodieSchema convertFIXED_LEN_BYTE_ARRAY(PrimitiveTypeName primitiveTypeName) {
              if (annotation instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
                return HoodieSchema.createUUID();
              } else {
                int size = parquetType.asPrimitiveType().getTypeLength();
                return HoodieSchema.createFixed(parquetType.getName(), null, null, size);
              }
            }
            @Override
            public HoodieSchema convertBINARY(PrimitiveTypeName primitiveTypeName) {
              if (annotation instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation ||
                  annotation instanceof  LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
                return HoodieSchema.create(HoodieSchemaType.STRING);
              } else {
                return HoodieSchema.create(HoodieSchemaType.BYTES);
              }
            }
          });

      return schema;

    } else {
      GroupType parquetGroupType = parquetType.asGroupType();
      LogicalTypeAnnotation logicalTypeAnnotation = parquetGroupType.getLogicalTypeAnnotation();
      if (logicalTypeAnnotation != null) {
        return logicalTypeAnnotation.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<HoodieSchema>() {
          @Override
          public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
            if (parquetGroupType.getFieldCount()!= 1) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            Type repeatedType = parquetGroupType.getType(0);
            if (!repeatedType.isRepetition(REPEATED)) {
              throw new UnsupportedOperationException("Invalid list type " + parquetGroupType);
            }
            if (isElementType(repeatedType, parquetGroupType.getName())) {
              // repeated element types are always required
              return java.util.Optional.of(HoodieSchema.createArray(convertField(repeatedType, names)));
            } else {
              Type elementType = repeatedType.asGroupType().getType(0);
              if (elementType.isRepetition(Type.Repetition.OPTIONAL)) {
                return java.util.Optional.of(HoodieSchema.createArray(optional(convertField(elementType, names))));
              } else {
                return java.util.Optional.of(HoodieSchema.createArray(convertField(elementType, names)));
              }
            }
          }

          @Override
          // for backward-compatibility
          public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
            return visitMapOrMapKeyValue();
          }

          @Override
          public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
            return visitMapOrMapKeyValue();
          }

          private java.util.Optional<HoodieSchema> visitMapOrMapKeyValue() {
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
              return java.util.Optional.of(HoodieSchema.createMap(optional(convertField(valueType, names))));
            } else {
              return java.util.Optional.of(HoodieSchema.createMap(convertField(valueType, names)));
            }
          }

          @Override
          public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
            return java.util.Optional.of(HoodieSchema.create(HoodieSchemaType.STRING));
          }
        }).orElseThrow(() -> new UnsupportedOperationException("Cannot convert Parquet type " + parquetType));
      } else {
        // if no original type then it's a record
        return convertFields(parquetGroupType.getName(), parquetGroupType.getFields(), names);
      }
    }
  }

  /**
   * Converts Parquet LogicalTypeAnnotation directly to HoodieSchema using HoodieSchema's factory methods.
   * This replaces the need to convert through Avro's LogicalType intermediate representation.
   * Also validates that logical types are only applied to compatible primitive types.
   *
   * @param annotation Parquet logical type annotation
   * @param parquetType The parquet type containing the annotation
   * @return HoodieSchema with the logical type applied, or null if no logical type conversion is needed
   * @throws IllegalArgumentException if the logical type is not compatible with the primitive type
   */
  private HoodieSchema convertLogicalTypeAnnotationToHoodieSchema(LogicalTypeAnnotation annotation, Type parquetType) {
    if (annotation == null) {
      return null;
    }

    final PrimitiveTypeName primitiveType = parquetType.asPrimitiveType().getPrimitiveTypeName();

    return annotation.accept(new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<HoodieSchema>() {
      @Override
      public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
        // DECIMAL can be BINARY or FIXED_LEN_BYTE_ARRAY
        if (primitiveType == FIXED_LEN_BYTE_ARRAY) {
          int fixedSize = parquetType.asPrimitiveType().getTypeLength();
          String name = parquetType.getName();
          return java.util.Optional.of(HoodieSchema.createDecimal(name, null, null,
              decimalLogicalType.getPrecision(), decimalLogicalType.getScale(), fixedSize));
        } else if (primitiveType == BINARY) {
          return java.util.Optional.of(HoodieSchema.createDecimal(decimalLogicalType.getPrecision(), decimalLogicalType.getScale()));
        } else {
          return java.util.Optional.empty();
        }
      }

      @Override
      public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
        // DATE must be INT32
        if (primitiveType != INT32) {
          throw new IllegalArgumentException("DATE can only annotate INT32, found " + primitiveType);
        }
        return java.util.Optional.of(HoodieSchema.createDate());
      }

      @Override
      public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
        LogicalTypeAnnotation.TimeUnit unit = timeLogicalType.getUnit();
        switch (unit) {
          case MILLIS:
            // TIME_MILLIS must be INT32
            if (primitiveType != INT32) {
              throw new IllegalArgumentException("TIME(MILLIS) can only annotate INT32, found " + primitiveType);
            }
            return java.util.Optional.of(HoodieSchema.createTimeMillis());
          case MICROS:
            // TIME_MICROS must be INT64
            if (primitiveType != INT64) {
              throw new IllegalArgumentException("TIME(MICROS) can only annotate INT64, found " + primitiveType);
            }
            return java.util.Optional.of(HoodieSchema.createTimeMicros());
          case NANOS:
            // Avro doesn't support nanosecond precision for time
            return java.util.Optional.empty();
          default:
            return java.util.Optional.empty();
        }
      }

      @Override
      public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
        LogicalTypeAnnotation.TimeUnit unit = timestampLogicalType.getUnit();
        boolean isAdjustedToUTC = timestampLogicalType.isAdjustedToUTC();

        // TIMESTAMP must be INT64
        if (primitiveType != INT64) {
          throw new IllegalArgumentException("TIMESTAMP can only annotate INT64, found " + primitiveType);
        }

        if (isAdjustedToUTC || !avroVersionSupportsLocalTimestampTypes()) {
          switch (unit) {
            case MILLIS:
              return java.util.Optional.of(HoodieSchema.createTimestampMillis());
            case MICROS:
              return java.util.Optional.of(HoodieSchema.createTimestampMicros());
            case NANOS:
              // Avro doesn't support nanosecond precision for timestamp
              return java.util.Optional.empty();
            default:
              return java.util.Optional.empty();
          }
        } else {
          switch (unit) {
            case MILLIS:
              return java.util.Optional.of(HoodieSchema.createLocalTimestampMillis());
            case MICROS:
              return java.util.Optional.of(HoodieSchema.createLocalTimestampMicros());
            case NANOS:
              // Avro doesn't support nanosecond precision for timestamp
              return java.util.Optional.empty();
            default:
              return java.util.Optional.empty();
          }
        }
      }

      @Override
      public java.util.Optional<HoodieSchema> visit(UUIDLogicalTypeAnnotation uuidLogicalType) {
        // UUID must be FIXED_LEN_BYTE_ARRAY with length 16
        if (primitiveType != FIXED_LEN_BYTE_ARRAY) {
          return java.util.Optional.empty();
        }
        return java.util.Optional.of(HoodieSchema.createUUID());
      }

      @Override
      public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
        // STRING must be BINARY
        if (primitiveType != BINARY) {
          return java.util.Optional.empty();
        }
        return java.util.Optional.of(HoodieSchema.create(HoodieSchemaType.STRING));
      }

      @Override
      public java.util.Optional<HoodieSchema> visit(LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
        // ENUM must be BINARY
        if (primitiveType != BINARY) {
          return java.util.Optional.empty();
        }
        return java.util.Optional.of(HoodieSchema.create(HoodieSchemaType.STRING));
      }

      // Return empty for other logical types that don't have direct HoodieSchema equivalents
      // They will fall back to the basic type conversion
    }).orElse(null);
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

  private static HoodieSchema optional(HoodieSchema original) {
    // null is first in the union because Parquet's default is always null
    return HoodieSchema.createUnion(Arrays.asList(
        HoodieSchema.create(HoodieSchemaType.NULL),
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
