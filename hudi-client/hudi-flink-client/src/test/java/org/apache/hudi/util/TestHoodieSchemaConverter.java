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
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieSchemaConverter}.
 */
public class TestHoodieSchemaConverter {

  @Test
  public void testPrimitiveTypes() {
    // String
    HoodieSchema stringSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.STRING().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.STRING, stringSchema.getType());

    // Int
    HoodieSchema intSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.INT().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.INT, intSchema.getType());

    // Long
    HoodieSchema longSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.BIGINT().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.LONG, longSchema.getType());

    // Float
    HoodieSchema floatSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.FLOAT().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.FLOAT, floatSchema.getType());

    // Double
    HoodieSchema doubleSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.DOUBLE().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.DOUBLE, doubleSchema.getType());

    // Boolean
    HoodieSchema boolSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.BOOLEAN().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.BOOLEAN, boolSchema.getType());

    // Bytes
    HoodieSchema bytesSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.BYTES().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.BYTES, bytesSchema.getType());
  }

  @Test
  public void testNullableTypes() {
    HoodieSchema nullableString = HoodieSchemaConverter.convertToSchema(
        DataTypes.STRING().nullable().getLogicalType());
    assertEquals(HoodieSchemaType.UNION, nullableString.getType());
    assertTrue(nullableString.isNullable());

    HoodieSchema nullableInt = HoodieSchemaConverter.convertToSchema(
        DataTypes.INT().nullable().getLogicalType());
    assertEquals(HoodieSchemaType.UNION, nullableInt.getType());
    assertTrue(nullableInt.isNullable());
  }

  @Test
  public void testTemporalTypes() {
    // Date
    HoodieSchema dateSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.DATE().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.DATE, dateSchema.getType());

    // Time
    HoodieSchema timeSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.TIME(3).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIME, timeSchema.getType());

    // Timestamp millis
    HoodieSchema timestampMillisSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.TIMESTAMP(3).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIMESTAMP, timestampMillisSchema.getType());
    assertTrue(timestampMillisSchema instanceof HoodieSchema.Timestamp);
    assertEquals(HoodieSchema.TimePrecision.MILLIS,
        ((HoodieSchema.Timestamp) timestampMillisSchema).getPrecision());

    // Timestamp micros
    HoodieSchema timestampMicrosSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.TIMESTAMP(6).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIMESTAMP, timestampMicrosSchema.getType());
    assertTrue(timestampMicrosSchema instanceof HoodieSchema.Timestamp);
    assertEquals(HoodieSchema.TimePrecision.MICROS,
        ((HoodieSchema.Timestamp) timestampMicrosSchema).getPrecision());

    // Local timestamp millis
    HoodieSchema localTimestampMillisSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIMESTAMP, localTimestampMillisSchema.getType());

    // Local timestamp micros
    HoodieSchema localTimestampMicrosSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIMESTAMP, localTimestampMicrosSchema.getType());
  }

  @Test
  public void testDecimalType() {
    HoodieSchema decimalSchema = HoodieSchemaConverter.convertToSchema(
        DataTypes.DECIMAL(10, 2).notNull().getLogicalType(), "test");
    assertEquals(HoodieSchemaType.DECIMAL, decimalSchema.getType());
    assertTrue(decimalSchema instanceof HoodieSchema.Decimal);

    HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) decimalSchema;
    assertEquals(10, decimal.getPrecision());
    assertEquals(2, decimal.getScale());
  }

  @Test
  public void testArrayType() {
    LogicalType arrayType = DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull().getLogicalType();
    HoodieSchema arraySchema = HoodieSchemaConverter.convertToSchema(arrayType);

    assertEquals(HoodieSchemaType.ARRAY, arraySchema.getType());
    assertEquals(HoodieSchemaType.STRING, arraySchema.getElementType().getType());
  }

  @Test
  public void testMapType() {
    LogicalType mapType = DataTypes.MAP(
        DataTypes.STRING().notNull(),
        DataTypes.INT().notNull()).notNull().getLogicalType();
    HoodieSchema mapSchema = HoodieSchemaConverter.convertToSchema(mapType);

    assertEquals(HoodieSchemaType.MAP, mapSchema.getType());
    assertEquals(HoodieSchemaType.INT, mapSchema.getValueType().getType());
  }

  @Test
  public void testRecordType() {
    LogicalType recordType = DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()),
        DataTypes.FIELD("name", DataTypes.STRING().notNull()),
        DataTypes.FIELD("age", DataTypes.INT().nullable())
    ).notNull().getLogicalType();

    HoodieSchema recordSchema = HoodieSchemaConverter.convertToSchema(recordType, "Person");

    assertEquals(HoodieSchemaType.RECORD, recordSchema.getType());
    assertEquals(3, recordSchema.getFields().size());
    assertEquals("id", recordSchema.getFields().get(0).name());
    assertEquals("name", recordSchema.getFields().get(1).name());
    assertEquals("age", recordSchema.getFields().get(2).name());

    // Verify nullable field
    assertTrue(recordSchema.getFields().get(2).schema().isNullable());
  }

  @Test
  public void testNestedRecordType() {
    LogicalType nestedRecordType = DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.INT().notNull()),
        DataTypes.FIELD("address", DataTypes.ROW(
            DataTypes.FIELD("street", DataTypes.STRING().notNull()),
            DataTypes.FIELD("city", DataTypes.STRING().notNull())
        ).notNull())
    ).notNull().getLogicalType();

    HoodieSchema nestedSchema = HoodieSchemaConverter.convertToSchema(nestedRecordType, "User");

    assertEquals(HoodieSchemaType.RECORD, nestedSchema.getType());
    assertEquals(2, nestedSchema.getFields().size());

    HoodieSchema addressSchema = nestedSchema.getFields().get(1).schema();
    assertEquals(HoodieSchemaType.RECORD, addressSchema.getType());
    assertEquals(2, addressSchema.getFields().size());
  }

  @Test
  public void testCompareWithAvroConversion() {
    // Test that HoodieSchemaConverter produces the same result as
    // AvroSchemaConverter + HoodieSchema.fromAvroSchema()

    RowType flinkRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
        DataTypes.FIELD("name", DataTypes.STRING().nullable()),
        DataTypes.FIELD("timestamp", DataTypes.TIMESTAMP(3).notNull()),
        DataTypes.FIELD("decimal_val", DataTypes.DECIMAL(10, 2).notNull())
    ).notNull().getLogicalType();

    // Method 1: Direct HoodieSchema conversion
    HoodieSchema directSchema = HoodieSchemaConverter.convertToSchema(flinkRowType, "TestRecord");

    // Method 2: Via Avro conversion
    HoodieSchema viaAvroSchema = HoodieSchema.fromAvroSchema(
        AvroSchemaConverter.convertToSchema(flinkRowType, "TestRecord"));

    // Both should produce equivalent schemas
    assertNotNull(directSchema);
    assertNotNull(viaAvroSchema);
    assertEquals(HoodieSchemaType.RECORD, directSchema.getType());
    assertEquals(HoodieSchemaType.RECORD, viaAvroSchema.getType());
    assertEquals(4, directSchema.getFields().size());
    assertEquals(4, viaAvroSchema.getFields().size());

    // Verify field types match
    for (int i = 0; i < 4; i++) {
      assertEquals(
          viaAvroSchema.getFields().get(i).schema().getType(),
          directSchema.getFields().get(i).schema().getType(),
          "Field " + i + " type mismatch"
      );
    }
  }

  @Test
  public void testComplexNestedStructure() {
    LogicalType complexType = DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
        DataTypes.FIELD("tags", DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull()),
        DataTypes.FIELD("metadata", DataTypes.MAP(
            DataTypes.STRING().notNull(),
            DataTypes.STRING().notNull()).notNull()),
        DataTypes.FIELD("nested", DataTypes.ROW(
            DataTypes.FIELD("value", DataTypes.DOUBLE().notNull()),
            DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
        ).notNull())
    ).notNull().getLogicalType();

    HoodieSchema complexSchema = HoodieSchemaConverter.convertToSchema(complexType, "ComplexRecord");

    assertNotNull(complexSchema);
    assertEquals(HoodieSchemaType.RECORD, complexSchema.getType());
    assertEquals(4, complexSchema.getFields().size());

    // Verify array field
    assertEquals(HoodieSchemaType.ARRAY, complexSchema.getFields().get(1).schema().getType());

    // Verify map field
    assertEquals(HoodieSchemaType.MAP, complexSchema.getFields().get(2).schema().getType());

    // Verify nested record
    HoodieSchema nestedRecord = complexSchema.getFields().get(3).schema();
    assertEquals(HoodieSchemaType.RECORD, nestedRecord.getType());
    assertEquals(2, nestedRecord.getFields().size());
  }

  @Test
  public void testNativeConversionMatchesAvroPath() {
    // Verify native conversion produces same result as Avro path
    RowType originalRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
        DataTypes.FIELD("name", DataTypes.STRING().nullable()),
        DataTypes.FIELD("age", DataTypes.INT().notNull())
    ).notNull().getLogicalType();

    HoodieSchema hoodieSchema = HoodieSchemaConverter.convertToSchema(originalRowType, "TestRecord");

    // Native conversion
    DataType nativeResult = HoodieSchemaConverter.convertToDataType(hoodieSchema);

    // Avro path (for comparison)
    DataType avroResult = AvroSchemaConverter.convertToDataType(hoodieSchema.getAvroSchema());

    assertEquals(avroResult.getLogicalType(), nativeResult.getLogicalType());
  }

  @Test
  public void testRoundTripConversion() {
    RowType originalRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
        DataTypes.FIELD("name", DataTypes.STRING().nullable()),
        DataTypes.FIELD("age", DataTypes.INT().notNull())
    ).notNull().getLogicalType();

    HoodieSchema hoodieSchema = HoodieSchemaConverter.convertToSchema(originalRowType, "TestRecord");
    RowType convertedRowType = HoodieSchemaConverter.convertToRowType(hoodieSchema);

    assertEquals(originalRowType, convertedRowType);
  }

  @Test
  public void testConvertPrimitiveTypesToDataType() {
    RowType flinkRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("string_col", DataTypes.STRING().notNull()),
        DataTypes.FIELD("int_col", DataTypes.INT().notNull()),
        DataTypes.FIELD("long_col", DataTypes.BIGINT().notNull()),
        DataTypes.FIELD("float_col", DataTypes.FLOAT().notNull()),
        DataTypes.FIELD("double_col", DataTypes.DOUBLE().notNull()),
        DataTypes.FIELD("boolean_col", DataTypes.BOOLEAN().notNull()),
        DataTypes.FIELD("bytes_col", DataTypes.BYTES().notNull())
    ).notNull().getLogicalType();

    HoodieSchema hoodieSchema = HoodieSchemaConverter.convertToSchema(flinkRowType);
    RowType result = HoodieSchemaConverter.convertToRowType(hoodieSchema);

    assertEquals(7, result.getFieldCount());
    assertEquals(flinkRowType, result);
  }

  @Test
  public void testConvertNullableTypesToDataType() {
    RowType flinkRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("nullable_string", DataTypes.STRING().nullable()),
        DataTypes.FIELD("nullable_int", DataTypes.INT().nullable())
    ).notNull().getLogicalType();

    HoodieSchema hoodieSchema = HoodieSchemaConverter.convertToSchema(flinkRowType);
    RowType result = HoodieSchemaConverter.convertToRowType(hoodieSchema);

    assertTrue(result.getTypeAt(0).isNullable());
    assertTrue(result.getTypeAt(1).isNullable());
    assertEquals(flinkRowType, result);
  }

  @Test
  public void testConvertTemporalTypesToDataType() {
    RowType flinkRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("date_col", DataTypes.DATE().notNull()),
        DataTypes.FIELD("time_col", DataTypes.TIME(3).notNull()),
        DataTypes.FIELD("timestamp_millis", DataTypes.TIMESTAMP(3).notNull()),
        DataTypes.FIELD("timestamp_micros", DataTypes.TIMESTAMP(6).notNull()),
        DataTypes.FIELD("local_timestamp_millis", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull()),
        DataTypes.FIELD("local_timestamp_micros", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull())
    ).notNull().getLogicalType();

    HoodieSchema hoodieSchema = HoodieSchemaConverter.convertToSchema(flinkRowType);
    RowType result = HoodieSchemaConverter.convertToRowType(hoodieSchema);

    assertEquals(flinkRowType, result);
    assertEquals(3, ((TimestampType) result.getTypeAt(2)).getPrecision());
    assertEquals(6, ((TimestampType) result.getTypeAt(3)).getPrecision());
  }

  @Test
  public void testConvertDecimalTypeToDataType() {
    RowType flinkRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("decimal_col", DataTypes.DECIMAL(10, 2).notNull())
    ).notNull().getLogicalType();

    HoodieSchema hoodieSchema = HoodieSchemaConverter.convertToSchema(flinkRowType);
    RowType result = HoodieSchemaConverter.convertToRowType(hoodieSchema);

    assertTrue(result.getTypeAt(0) instanceof DecimalType);
    DecimalType decimal = (DecimalType) result.getTypeAt(0);
    assertEquals(10, decimal.getPrecision());
    assertEquals(2, decimal.getScale());
  }

  @Test
  public void testConvertComplexTypesToDataType() {
    RowType flinkRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("array_col", DataTypes.ARRAY(DataTypes.STRING().notNull()).notNull()),
        DataTypes.FIELD("map_col", DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.INT().notNull()).notNull()),
        DataTypes.FIELD("nested_record", DataTypes.ROW(
            DataTypes.FIELD("nested_id", DataTypes.INT().notNull()),
            DataTypes.FIELD("nested_name", DataTypes.STRING().notNull())
        ).notNull())
    ).notNull().getLogicalType();

    HoodieSchema hoodieSchema = HoodieSchemaConverter.convertToSchema(flinkRowType);
    RowType result = HoodieSchemaConverter.convertToRowType(hoodieSchema);

    assertEquals(flinkRowType, result);
    assertTrue(result.getTypeAt(0) instanceof ArrayType);
    assertTrue(result.getTypeAt(1) instanceof MapType);
    assertTrue(result.getTypeAt(2) instanceof RowType);
  }

  @Test
  public void testConvertNullSchemaThrowsException() {
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaConverter.convertToRowType(null);
    });
  }

  @Test
  public void testConvertNonRecordSchemaThrowsException() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaConverter.convertToRowType(stringSchema);
    });
  }

  @Test
  public void testEnumToStringConversion() {
    HoodieSchema enumSchema = HoodieSchema.createEnum(
        "Color", null, null, Arrays.asList("RED", "GREEN", "BLUE"));

    DataType dataType = HoodieSchemaConverter.convertToDataType(enumSchema);
    assertTrue(dataType.getLogicalType() instanceof VarCharType);
  }

  @Test
  public void testFixedConversion() {
    HoodieSchema fixedSchema = HoodieSchema.createFixed("MD5", null, null, 16);
    DataType dataType = HoodieSchemaConverter.convertToDataType(fixedSchema);
    assertTrue(dataType.getLogicalType() instanceof VarBinaryType);
  }
}