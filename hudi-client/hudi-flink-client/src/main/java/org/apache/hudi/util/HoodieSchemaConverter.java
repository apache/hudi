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

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.ReflectionUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts Flink's LogicalType into HoodieSchema.
 */
public class HoodieSchemaConverter {

  /**
   * Converts a Flink LogicalType into a HoodieSchema.
   *
   * <p>Uses "record" as the default type name for record types.
   *
   * @param logicalType Flink logical type definition
   * @return HoodieSchema matching the logical type
   */
  public static HoodieSchema convertToSchema(LogicalType logicalType) {
    return convertToSchema(logicalType, "record");
  }

  /**
   * Converts a Flink LogicalType into a HoodieSchema with specified record name.
   *
   * <p>The "{rowName}." is used as the nested row type name prefix in order to generate
   * the right schema. Nested record types that only differ by type name are still compatible.
   *
   * @param logicalType Flink logical type
   * @param rowName     the record name
   * @return HoodieSchema matching this logical type
   */
  public static HoodieSchema convertToSchema(LogicalType logicalType, String rowName) {
    int precision;
    boolean nullable = logicalType.isNullable();

    switch (logicalType.getTypeRoot()) {
      case NULL:
        return HoodieSchema.create(HoodieSchemaType.NULL);

      case BOOLEAN:
        HoodieSchema bool = HoodieSchema.create(HoodieSchemaType.BOOLEAN);
        return nullable ? HoodieSchema.createNullable(bool) : bool;

      case TINYINT:
      case SMALLINT:
      case INTEGER:
        HoodieSchema integer = HoodieSchema.create(HoodieSchemaType.INT);
        return nullable ? HoodieSchema.createNullable(integer) : integer;

      case BIGINT:
        HoodieSchema bigint = HoodieSchema.create(HoodieSchemaType.LONG);
        return nullable ? HoodieSchema.createNullable(bigint) : bigint;

      case FLOAT:
        HoodieSchema f = HoodieSchema.create(HoodieSchemaType.FLOAT);
        return nullable ? HoodieSchema.createNullable(f) : f;

      case DOUBLE:
        HoodieSchema d = HoodieSchema.create(HoodieSchemaType.DOUBLE);
        return nullable ? HoodieSchema.createNullable(d) : d;

      case CHAR:
      case VARCHAR:
        HoodieSchema str = HoodieSchema.create(HoodieSchemaType.STRING);
        return nullable ? HoodieSchema.createNullable(str) : str;

      case BINARY:
      case VARBINARY:
        HoodieSchema binary = HoodieSchema.create(HoodieSchemaType.BYTES);
        return nullable ? HoodieSchema.createNullable(binary) : binary;

      case TIMESTAMP_WITHOUT_TIME_ZONE:
        final TimestampType timestampType = (TimestampType) logicalType;
        precision = timestampType.getPrecision();
        HoodieSchema timestamp;
        if (precision <= 3) {
          timestamp = HoodieSchema.createTimestampMillis();
        } else if (precision <= 6) {
          timestamp = HoodieSchema.createTimestampMicros();
        } else {
          throw new IllegalArgumentException(
              "HoodieSchema does not support TIMESTAMP type with precision: "
                  + precision
                  + ", it only supports precisions <= 6.");
        }
        return nullable ? HoodieSchema.createNullable(timestamp) : timestamp;

      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        final LocalZonedTimestampType localZonedTimestampType = (LocalZonedTimestampType) logicalType;
        precision = localZonedTimestampType.getPrecision();
        HoodieSchema localTimestamp;
        if (precision <= 3) {
          localTimestamp = HoodieSchema.createLocalTimestampMillis();
        } else if (precision <= 6) {
          localTimestamp = HoodieSchema.createLocalTimestampMicros();
        } else {
          throw new IllegalArgumentException(
              "HoodieSchema does not support LOCAL TIMESTAMP type with precision: "
                  + precision
                  + ", it only supports precisions <= 6.");
        }
        return nullable ? HoodieSchema.createNullable(localTimestamp) : localTimestamp;

      case DATE:
        HoodieSchema date = HoodieSchema.createDate();
        return nullable ? HoodieSchema.createNullable(date) : date;

      case TIME_WITHOUT_TIME_ZONE:
        precision = ((TimeType) logicalType).getPrecision();
        HoodieSchema time;
        if (precision <= 3) {
          time = HoodieSchema.createTimeMillis();
        } else if (precision <= 6) {
          time = HoodieSchema.createTimeMicros();
        } else {
          throw new IllegalArgumentException(
              "HoodieSchema does not support TIME type with precision: "
                  + precision
                  + ", maximum precision is 6 (microseconds).");
        }
        return nullable ? HoodieSchema.createNullable(time) : time;

      case DECIMAL:
        DecimalType decimalType = (DecimalType) logicalType;
        int fixedSize = computeMinBytesForDecimalPrecision(decimalType.getPrecision());
        HoodieSchema decimal = HoodieSchema.createDecimal(
            String.format("%s.fixed", rowName),
            null,
            null,
            decimalType.getPrecision(),
            decimalType.getScale(),
            fixedSize
        );
        return nullable ? HoodieSchema.createNullable(decimal) : decimal;

      case ROW:
        RowType rowType = (RowType) logicalType;
        List<String> fieldNames = rowType.getFieldNames();

        List<HoodieSchemaField> hoodieFields = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
          String fieldName = fieldNames.get(i);
          LogicalType fieldType = rowType.getTypeAt(i);

          // Recursive call for field schema
          HoodieSchema fieldSchema = convertToSchema(fieldType, rowName + "." + fieldName);

          // Create field with or without default value
          HoodieSchemaField field;
          if (fieldType.isNullable()) {
            field = HoodieSchemaField.of(fieldName, fieldSchema, null, HoodieSchema.NULL_VALUE);
          } else {
            field = HoodieSchemaField.of(fieldName, fieldSchema);
          }
          hoodieFields.add(field);
        }

        HoodieSchema record = HoodieSchema.createRecord(rowName, null, null, hoodieFields);
        return nullable ? HoodieSchema.createNullable(record) : record;

      case MULTISET:
      case MAP:
        LogicalType valueType = extractValueTypeForMap(logicalType);
        HoodieSchema valueSchema = convertToSchema(valueType, rowName);
        HoodieSchema map = HoodieSchema.createMap(valueSchema);
        return nullable ? HoodieSchema.createNullable(map) : map;

      case ARRAY:
        ArrayType arrayType = (ArrayType) logicalType;
        HoodieSchema elementSchema = convertToSchema(arrayType.getElementType(), rowName);
        HoodieSchema array = HoodieSchema.createArray(elementSchema);
        return nullable ? HoodieSchema.createNullable(array) : array;

      case RAW:
      default:
        throw new UnsupportedOperationException(
            "Unsupported type for HoodieSchema conversion: " + logicalType);
    }
  }

  /**
   * Extracts value type for map conversion.
   * Maps must have string keys for Avro/HoodieSchema compatibility.
   */
  private static LogicalType extractValueTypeForMap(LogicalType type) {
    LogicalType keyType;
    LogicalType valueType;
    if (type instanceof MapType) {
      MapType mapType = (MapType) type;
      keyType = mapType.getKeyType();
      valueType = mapType.getValueType();
    } else {
      MultisetType multisetType = (MultisetType) type;
      keyType = multisetType.getElementType();
      valueType = new IntType();
    }
    if (!isFamily(keyType, LogicalTypeFamily.CHARACTER_STRING)) {
      throw new UnsupportedOperationException(
          "HoodieSchema doesn't support non-string as key type of map. "
              + "The key type is: "
              + keyType.asSummaryString());
    }
    return valueType;
  }

  /**
   * Returns whether the given logical type belongs to the family.
   */
  private static boolean isFamily(LogicalType logicalType, LogicalTypeFamily family) {
    return logicalType.getTypeRoot().getFamilies().contains(family);
  }

  /**
   * Computes minimum bytes needed for decimal precision.
   * This ensures compatibility with Avro fixed-size decimal representation.
   */
  private static int computeMinBytesForDecimalPrecision(int precision) {
    int numBytes = 1;
    while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
      numBytes += 1;
    }
    return numBytes;
  }

  // ===== Conversion from HoodieSchema to Flink DataType =====

  /**
   * Converts a HoodieSchema into Flink's DataType.
   *
   * <p>This method provides native conversion from HoodieSchema to Flink DataType
   * without going through Avro intermediate representation, future-proofing the
   * implementation against changes in the Avro layer.
   *
   * @param hoodieSchema the HoodieSchema to convert
   * @return Flink DataType matching the schema
   * @throws IllegalArgumentException if the schema contains unsupported types
   */
  public static DataType convertToDataType(HoodieSchema hoodieSchema) {
    if (hoodieSchema == null) {
      throw new IllegalArgumentException("HoodieSchema cannot be null");
    }

    HoodieSchemaType type = hoodieSchema.getType();

    switch (type) {
      case NULL:
        return DataTypes.NULL();
      case BOOLEAN:
        return DataTypes.BOOLEAN().notNull();
      case INT:
        return DataTypes.INT().notNull();
      case LONG:
        return DataTypes.BIGINT().notNull();
      case FLOAT:
        return DataTypes.FLOAT().notNull();
      case DOUBLE:
        return DataTypes.DOUBLE().notNull();
      case BYTES:
        return DataTypes.BYTES().notNull();
      case STRING:
        return DataTypes.STRING().notNull();
      case ENUM:
        // Flink doesn't have native enum type, convert to STRING
        return DataTypes.STRING().notNull();
      case FIXED:
        return DataTypes.VARBINARY(hoodieSchema.getFixedSize()).notNull();
      case DECIMAL:
        return convertDecimal(hoodieSchema);
      case DATE:
        return DataTypes.DATE().notNull();
      case TIME:
        return convertTime(hoodieSchema);
      case TIMESTAMP:
        return convertTimestamp(hoodieSchema);
      case UUID:
        return DataTypes.STRING().notNull();
      case ARRAY:
        return convertArray(hoodieSchema);
      case MAP:
        return convertMap(hoodieSchema);
      case RECORD:
        return convertRecord(hoodieSchema);
      case UNION:
        return convertUnion(hoodieSchema);
      default:
        throw new IllegalArgumentException("Unsupported HoodieSchemaType: " + type);
    }
  }

  /**
   * Converts a HoodieSchema (RECORD type) into a Flink RowType.
   *
   * @param schema HoodieSchema to convert (must be a RECORD type)
   * @return RowType matching the HoodieSchema structure
   * @throws IllegalArgumentException if schema is null or not a RECORD type
   */
  public static RowType convertToRowType(HoodieSchema schema) {
    if (schema == null) {
      throw new IllegalArgumentException("HoodieSchema cannot be null");
    }
    if (schema.getType() != HoodieSchemaType.RECORD) {
      throw new IllegalArgumentException(
          "Only RECORD type schemas can be converted to RowType, got: " + schema.getType());
    }

    DataType dataType = convertToDataType(schema);
    return (RowType) dataType.getLogicalType();
  }

  private static DataType convertDecimal(HoodieSchema schema) {
    if (!(schema instanceof HoodieSchema.Decimal)) {
      throw new IllegalStateException("Expected HoodieSchema.Decimal but got: " + schema.getClass());
    }
    HoodieSchema.Decimal decimalSchema = (HoodieSchema.Decimal) schema;
    return DataTypes.DECIMAL(decimalSchema.getPrecision(), decimalSchema.getScale()).notNull();
  }

  private static DataType convertTimestamp(HoodieSchema schema) {
    if (!(schema instanceof HoodieSchema.Timestamp)) {
      throw new IllegalStateException("Expected HoodieSchema.Timestamp but got: " + schema.getClass());
    }
    HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) schema;
    int flinkPrecision = (timestampSchema.getPrecision() == HoodieSchema.TimePrecision.MILLIS) ? 3 : 6;

    if (timestampSchema.isUtcAdjusted()) {
      return DataTypes.TIMESTAMP(flinkPrecision).notNull();
    } else {
      return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(flinkPrecision).notNull();
    }
  }

  private static DataType convertTime(HoodieSchema schema) {
    if (!(schema instanceof HoodieSchema.Time)) {
      throw new IllegalStateException("Expected HoodieSchema.Time but got: " + schema.getClass());
    }
    HoodieSchema.Time timeSchema = (HoodieSchema.Time) schema;
    int flinkPrecision = (timeSchema.getPrecision() == HoodieSchema.TimePrecision.MILLIS) ? 3 : 6;
    return DataTypes.TIME(flinkPrecision).notNull();
  }

  private static DataType convertRecord(HoodieSchema schema) {
    List<HoodieSchemaField> fields = schema.getFields();
    DataTypes.Field[] flinkFields = new DataTypes.Field[fields.size()];

    for (int i = 0; i < fields.size(); i++) {
      HoodieSchemaField field = fields.get(i);
      DataType fieldType = convertToDataType(field.schema());
      flinkFields[i] = DataTypes.FIELD(field.name(), fieldType);
    }

    return DataTypes.ROW(flinkFields).notNull();
  }

  private static DataType convertArray(HoodieSchema schema) {
    HoodieSchema elementSchema = schema.getElementType();
    DataType elementType = convertToDataType(elementSchema);
    return DataTypes.ARRAY(elementType).notNull();
  }

  private static DataType convertMap(HoodieSchema schema) {
    HoodieSchema valueSchema = schema.getValueType();
    DataType valueType = convertToDataType(valueSchema);
    return DataTypes.MAP(DataTypes.STRING().notNull(), valueType).notNull();
  }

  private static DataType convertUnion(HoodieSchema schema) {
    List<HoodieSchema> unionTypes = schema.getTypes();

    // Simple nullable union [null, T]
    if (schema.isNullable() && unionTypes.size() == 2) {
      HoodieSchema nonNullType = schema.getNonNullType();
      DataType converted = convertToDataType(nonNullType);
      return converted.nullable();
    }

    // Single-type union
    if (unionTypes.size() == 1) {
      return convertToDataType(unionTypes.get(0));
    }

    // Complex multi-type unions - use RAW type (matches AvroSchemaConverter logic)
    List<HoodieSchema> nonNullTypes = unionTypes.stream()
        .filter(t -> t.getType() != HoodieSchemaType.NULL)
        .collect(Collectors.toList());

    boolean nullable = unionTypes.size() > nonNullTypes.size();

    // Use RAW type for complex unions
    DataType rawDataType = (DataType) ReflectionUtils.invokeStaticMethod(
        "org.apache.hudi.utils.DataTypeUtils",
        "createAtomicRawType",
        new Object[] {false, Types.GENERIC(Object.class)},
        Boolean.class,
        TypeInformation.class);

    if (recordTypesOfSameNumFields(nonNullTypes)) {
      DataType converted = DataTypes.ROW(
              DataTypes.FIELD("wrapper", rawDataType))
          .notNull();
      return nullable ? converted.nullable() : converted;
    }

    return nullable ? rawDataType.nullable() : rawDataType;
  }

  /**
   * Returns true if all the types are RECORD type with same number of fields.
   */
  private static boolean recordTypesOfSameNumFields(List<HoodieSchema> types) {
    if (types == null || types.isEmpty()) {
      return false;
    }
    if (types.stream().anyMatch(s -> s.getType() != HoodieSchemaType.RECORD)) {
      return false;
    }
    int numFields = types.get(0).getFields().size();
    return types.stream().allMatch(s -> s.getFields().size() == numFields);
  }
}