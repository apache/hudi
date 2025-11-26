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
  public static HoodieSchema convertToHoodieSchema(LogicalType logicalType) {
    return convertToHoodieSchema(logicalType, "record");
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
  public static HoodieSchema convertToHoodieSchema(LogicalType logicalType, String rowName) {
    int precision;
    boolean nullable = logicalType.isNullable();

    switch (logicalType.getTypeRoot()) {
      case NULL:
        return HoodieSchema.create(HoodieSchemaType.NULL);

      case BOOLEAN:
        HoodieSchema bool = HoodieSchema.create(HoodieSchemaType.BOOLEAN);
        return nullable ? nullableSchema(bool) : bool;

      case TINYINT:
      case SMALLINT:
      case INTEGER:
        HoodieSchema integer = HoodieSchema.create(HoodieSchemaType.INT);
        return nullable ? nullableSchema(integer) : integer;

      case BIGINT:
        HoodieSchema bigint = HoodieSchema.create(HoodieSchemaType.LONG);
        return nullable ? nullableSchema(bigint) : bigint;

      case FLOAT:
        HoodieSchema f = HoodieSchema.create(HoodieSchemaType.FLOAT);
        return nullable ? nullableSchema(f) : f;

      case DOUBLE:
        HoodieSchema d = HoodieSchema.create(HoodieSchemaType.DOUBLE);
        return nullable ? nullableSchema(d) : d;

      case CHAR:
      case VARCHAR:
        HoodieSchema str = HoodieSchema.create(HoodieSchemaType.STRING);
        return nullable ? nullableSchema(str) : str;

      case BINARY:
      case VARBINARY:
        HoodieSchema binary = HoodieSchema.create(HoodieSchemaType.BYTES);
        return nullable ? nullableSchema(binary) : binary;

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
        return nullable ? nullableSchema(timestamp) : timestamp;

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
        return nullable ? nullableSchema(localTimestamp) : localTimestamp;

      case DATE:
        HoodieSchema date = HoodieSchema.createDate();
        return nullable ? nullableSchema(date) : date;

      case TIME_WITHOUT_TIME_ZONE:
        precision = ((TimeType) logicalType).getPrecision();
        if (precision > 3) {
          throw new IllegalArgumentException(
              "HoodieSchema does not support TIME type with precision: "
                  + precision
                  + ", it only supports precision <= 3.");
        }
        HoodieSchema time = HoodieSchema.createTimeMillis();
        return nullable ? nullableSchema(time) : time;

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
        return nullable ? nullableSchema(decimal) : decimal;

      case ROW:
        RowType rowType = (RowType) logicalType;
        List<String> fieldNames = rowType.getFieldNames();

        List<HoodieSchemaField> hoodieFields = new ArrayList<>();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
          String fieldName = fieldNames.get(i);
          LogicalType fieldType = rowType.getTypeAt(i);

          // Recursive call for field schema
          HoodieSchema fieldSchema = convertToHoodieSchema(fieldType, rowName + "." + fieldName);

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
        return nullable ? nullableSchema(record) : record;

      case MULTISET:
      case MAP:
        LogicalType valueType = extractValueTypeForMap(logicalType);
        HoodieSchema valueSchema = convertToHoodieSchema(valueType, rowName);
        HoodieSchema map = HoodieSchema.createMap(valueSchema);
        return nullable ? nullableSchema(map) : map;

      case ARRAY:
        ArrayType arrayType = (ArrayType) logicalType;
        HoodieSchema elementSchema = convertToHoodieSchema(arrayType.getElementType(), rowName);
        HoodieSchema array = HoodieSchema.createArray(elementSchema);
        return nullable ? nullableSchema(array) : array;

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
   * Returns schema with nullable wrapper.
   */
  private static HoodieSchema nullableSchema(HoodieSchema schema) {
    return schema.isNullable()
        ? schema
        : HoodieSchema.createNullable(schema);
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
}