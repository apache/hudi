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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieSchemaConverter}.
 */
public class TestHoodieSchemaConverter {

  @Test
  public void testPrimitiveTypes() {
    // String
    HoodieSchema stringSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.STRING().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.STRING, stringSchema.getType());

    // Int
    HoodieSchema intSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.INT().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.INT, intSchema.getType());

    // Long
    HoodieSchema longSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.BIGINT().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.LONG, longSchema.getType());

    // Float
    HoodieSchema floatSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.FLOAT().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.FLOAT, floatSchema.getType());

    // Double
    HoodieSchema doubleSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.DOUBLE().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.DOUBLE, doubleSchema.getType());

    // Boolean
    HoodieSchema boolSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.BOOLEAN().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.BOOLEAN, boolSchema.getType());

    // Bytes
    HoodieSchema bytesSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.BYTES().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.BYTES, bytesSchema.getType());
  }

  @Test
  public void testNullableTypes() {
    HoodieSchema nullableString = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.STRING().nullable().getLogicalType());
    assertEquals(HoodieSchemaType.UNION, nullableString.getType());
    assertTrue(nullableString.isNullable());

    HoodieSchema nullableInt = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.INT().nullable().getLogicalType());
    assertEquals(HoodieSchemaType.UNION, nullableInt.getType());
    assertTrue(nullableInt.isNullable());
  }

  @Test
  public void testTemporalTypes() {
    // Date
    HoodieSchema dateSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.DATE().notNull().getLogicalType());
    assertEquals(HoodieSchemaType.DATE, dateSchema.getType());

    // Time
    HoodieSchema timeSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.TIME(3).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIME, timeSchema.getType());

    // Timestamp millis
    HoodieSchema timestampMillisSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.TIMESTAMP(3).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIMESTAMP, timestampMillisSchema.getType());
    assertTrue(timestampMillisSchema instanceof HoodieSchema.Timestamp);
    assertEquals(HoodieSchema.TimePrecision.MILLIS,
        ((HoodieSchema.Timestamp) timestampMillisSchema).getPrecision());

    // Timestamp micros
    HoodieSchema timestampMicrosSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.TIMESTAMP(6).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIMESTAMP, timestampMicrosSchema.getType());
    assertTrue(timestampMicrosSchema instanceof HoodieSchema.Timestamp);
    assertEquals(HoodieSchema.TimePrecision.MICROS,
        ((HoodieSchema.Timestamp) timestampMicrosSchema).getPrecision());

    // Local timestamp millis
    HoodieSchema localTimestampMillisSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIMESTAMP, localTimestampMillisSchema.getType());

    // Local timestamp micros
    HoodieSchema localTimestampMicrosSchema = HoodieSchemaConverter.convertToHoodieSchema(
        DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6).notNull().getLogicalType());
    assertEquals(HoodieSchemaType.TIMESTAMP, localTimestampMicrosSchema.getType());
  }

  @Test
  public void testDecimalType() {
    HoodieSchema decimalSchema = HoodieSchemaConverter.convertToHoodieSchema(
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
    HoodieSchema arraySchema = HoodieSchemaConverter.convertToHoodieSchema(arrayType);

    assertEquals(HoodieSchemaType.ARRAY, arraySchema.getType());
    assertEquals(HoodieSchemaType.STRING, arraySchema.getElementType().getType());
  }

  @Test
  public void testMapType() {
    LogicalType mapType = DataTypes.MAP(
        DataTypes.STRING().notNull(),
        DataTypes.INT().notNull()).notNull().getLogicalType();
    HoodieSchema mapSchema = HoodieSchemaConverter.convertToHoodieSchema(mapType);

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

    HoodieSchema recordSchema = HoodieSchemaConverter.convertToHoodieSchema(recordType, "Person");

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

    HoodieSchema nestedSchema = HoodieSchemaConverter.convertToHoodieSchema(nestedRecordType, "User");

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
    HoodieSchema directSchema = HoodieSchemaConverter.convertToHoodieSchema(flinkRowType, "TestRecord");

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

    HoodieSchema complexSchema = HoodieSchemaConverter.convertToHoodieSchema(complexType, "ComplexRecord");

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
}