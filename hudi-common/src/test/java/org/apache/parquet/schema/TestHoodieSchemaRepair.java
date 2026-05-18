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

package org.apache.parquet.schema;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieSchemaRepair}.
 */
public class TestHoodieSchemaRepair {

  @Test
  public void testNoRepairNeededIdenticalSchemas() {
    HoodieSchema requestedSchema = HoodieSchema.create(HoodieSchemaType.LONG);
    HoodieSchema tableSchema = HoodieSchema.create(HoodieSchemaType.LONG);

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "When schemas are identical, should return same instance");
  }

  @Test
  public void testNoRepairNeededDifferentPrimitiveTypes() {
    HoodieSchema requestedSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema tableSchema = HoodieSchema.create(HoodieSchemaType.INT);

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "When types differ, should return original schema");
  }

  @Test
  public void testRepairLongWithoutLogicalTypeToLocalTimestampMillis() {
    HoodieSchema requestedSchema = HoodieSchema.create(HoodieSchemaType.LONG);
    HoodieSchema tableSchema = HoodieSchema.createLocalTimestampMillis();

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create a new schema with logical type");
    assertEquals(HoodieSchemaType.TIMESTAMP, result.getType());
    HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) result;
    assertFalse(timestampSchema.isUtcAdjusted(), "Should be local timestamp");
    assertEquals(HoodieSchema.TimePrecision.MILLIS, timestampSchema.getPrecision());
  }

  @Test
  public void testRepairLongWithoutLogicalTypeToLocalTimestampMicros() {
    HoodieSchema requestedSchema = HoodieSchema.create(HoodieSchemaType.LONG);
    HoodieSchema tableSchema = HoodieSchema.createLocalTimestampMicros();

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create a new schema with logical type");
    assertEquals(HoodieSchemaType.TIMESTAMP, result.getType());
    HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) result;
    assertFalse(timestampSchema.isUtcAdjusted(), "Should be local timestamp");
    assertEquals(HoodieSchema.TimePrecision.MICROS, timestampSchema.getPrecision());
  }

  @Test
  public void testRepairTimestampMicrosToTimestampMillis() {
    HoodieSchema requestedSchema = HoodieSchema.createTimestampMicros();
    HoodieSchema tableSchema = HoodieSchema.createTimestampMillis();

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create a new schema with timestamp-millis");
    assertEquals(HoodieSchemaType.TIMESTAMP, result.getType());
    HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) result;
    assertTrue(timestampSchema.isUtcAdjusted(), "Should be timestamp");
    assertEquals(HoodieSchema.TimePrecision.MILLIS, timestampSchema.getPrecision());
  }

  @Test
  public void testNoRepairNeededTimestampMillisToTimestampMicros() {
    // This direction should NOT trigger repair
    HoodieSchema requestedSchema = HoodieSchema.createTimestampMillis();
    HoodieSchema tableSchema = HoodieSchema.createTimestampMicros();

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should not repair timestamp-millis to timestamp-micros");
  }

  @Test
  public void testNoRepairNeededNonLongTypes() {
    HoodieSchema requestedSchema = HoodieSchema.create(HoodieSchemaType.INT);
    HoodieSchema tableSchema = HoodieSchema.createDate();

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should not repair non-LONG types");
  }

  @Test
  public void testRepairNullableSchemaLongToLocalTimestampMillis() {
    HoodieSchema requestedSchema = HoodieSchema.createNullable(HoodieSchemaType.LONG);
    HoodieSchema tableSchema = HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMillis());

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new nullable schema with repaired type");
    assertEquals(HoodieSchemaType.UNION, result.getType());
    assertEquals(2, result.getTypes().size());

    HoodieSchema.Timestamp nonNullType = (HoodieSchema.Timestamp) result.getNonNullType();
    assertFalse(nonNullType.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, nonNullType.getPrecision());
  }

  @Test
  public void testRepairNullableSchemaTimestampMicrosToMillis() {
    HoodieSchema requestedSchema = HoodieSchema.createNullable(HoodieSchema.createTimestampMicros());
    HoodieSchema tableSchema = HoodieSchema.createNullable(HoodieSchema.createTimestampMillis());

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new nullable schema");
    assertEquals(HoodieSchemaType.UNION, result.getType());

    HoodieSchema.Timestamp nonNullType = (HoodieSchema.Timestamp) result.getNonNullType();
    assertTrue(nonNullType.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, nonNullType.getPrecision());
  }

  @Test
  public void testRepairRecordSingleField() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new record schema");
    assertEquals(HoodieSchemaType.RECORD, result.getType());
    assertEquals("TestRecord", result.getName());
    assertEquals(1, result.getFields().size());

    HoodieSchemaField field = result.getField("timestamp").get();
    HoodieSchema.Timestamp nonNullType = (HoodieSchema.Timestamp) field.getNonNullSchema();
    assertFalse(nonNullType.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, nonNullType.getPrecision());
  }

  @Test
  public void testRepairRecordMultipleFieldsOnlyOneNeedsRepair() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMicros(), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new record schema");
    assertEquals(3, result.getFields().size());

    // Verify id field unchanged - should be same schema instance
    assertSame(requestedSchema.getField("id").get().schema(), result.getField("id").get().schema());

    // Verify timestamp field repaired
    HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) result.getField("timestamp").get().getNonNullSchema();
    assertFalse(timestampSchema.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MICROS, timestampSchema.getPrecision());

    // Verify name field unchanged - should be same schema instance
    assertSame(requestedSchema.getField("name").get().schema(), result.getField("name").get().schema());
  }

  @Test
  public void testRepairRecordNestedRecord() {
    HoodieSchema nestedRequestedSchema = HoodieSchema.createRecord("NestedRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));

    HoodieSchema nestedTableSchema = HoodieSchema.createRecord("NestedRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)));

    HoodieSchema requestedSchema = HoodieSchema.createRecord("OuterRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("nested", nestedRequestedSchema, null, null)));

    HoodieSchema tableSchema = HoodieSchema.createRecord("OuterRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("nested", nestedTableSchema, null, null)));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new schema for nested record");

    // Verify id field unchanged - should be same schema instance
    assertSame(requestedSchema.getField("id").get().schema(), result.getField("id").get().schema());

    // Verify nested record was repaired
    HoodieSchema nestedResult = result.getField("nested").get().schema();
    assertEquals(HoodieSchemaType.RECORD, nestedResult.getType());
    HoodieSchema.Timestamp timestamp = (HoodieSchema.Timestamp) nestedResult.getField("timestamp").get().getNonNullSchema();
    assertFalse(timestamp.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, timestamp.getPrecision());
  }

  @Test
  public void testRepairRecordNullableNestedField() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp", HoodieSchema.createNullable(HoodieSchemaType.LONG), null, null)));

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp", HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMillis()), null, null)));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new schema");

    HoodieSchema fieldSchema = result.getField("timestamp").get().schema();
    assertEquals(HoodieSchemaType.UNION, fieldSchema.getType());

    HoodieSchema.Timestamp nonNullType = (HoodieSchema.Timestamp) fieldSchema.getNonNullType();
    assertFalse(nonNullType.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, nonNullType.getPrecision());
  }

  @Test
  public void testRepairArrayElementNeedsRepair() {
    HoodieSchema requestedSchema = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema tableSchema = HoodieSchema.createArray(HoodieSchema.createLocalTimestampMillis());

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new array schema");
    assertEquals(HoodieSchemaType.ARRAY, result.getType());
    HoodieSchema.Timestamp elementSchema = (HoodieSchema.Timestamp) result.getElementType();
    assertFalse(elementSchema.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, elementSchema.getPrecision());
  }

  @Test
  public void testRepairArrayNoRepairNeeded() {
    HoodieSchema elementSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema requestedSchema = HoodieSchema.createArray(elementSchema);
    HoodieSchema tableSchema = HoodieSchema.createArray(elementSchema);

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should return same array when no repair needed");
  }

  @Test
  public void testRepairArrayNullableElements() {
    HoodieSchema requestedSchema = HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchemaType.LONG));
    HoodieSchema tableSchema = HoodieSchema.createArray(HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMicros()));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new array schema");
    HoodieSchema elementSchema = result.getElementType();
    assertEquals(HoodieSchemaType.UNION, elementSchema.getType());

    HoodieSchema.Timestamp nonNullType = (HoodieSchema.Timestamp) elementSchema.getNonNullType();
    assertFalse(nonNullType.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MICROS, nonNullType.getPrecision());
  }

  @Test
  public void testRepairMapValueNeedsRepair() {
    HoodieSchema requestedSchema = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema tableSchema = HoodieSchema.createMap(HoodieSchema.createLocalTimestampMillis());

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new map schema");
    assertEquals(HoodieSchemaType.MAP, result.getType());
    HoodieSchema.Timestamp valueSchema = (HoodieSchema.Timestamp) result.getValueType();
    assertFalse(valueSchema.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, valueSchema.getPrecision());
  }

  @Test
  public void testRepairMapNoRepairNeeded() {
    HoodieSchema valueSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema requestedSchema = HoodieSchema.createMap(valueSchema);
    HoodieSchema tableSchema = HoodieSchema.createMap(valueSchema);

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should return same map when no repair needed");
  }

  @Test
  public void testRepairMapNullableValues() {
    HoodieSchema requestedSchema = HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchemaType.LONG));
    HoodieSchema tableSchema = HoodieSchema.createMap(HoodieSchema.createNullable(HoodieSchema.createLocalTimestampMillis()));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new map schema");
    HoodieSchema valueSchema = result.getValueType();
    assertEquals(HoodieSchemaType.UNION, valueSchema.getType());

    HoodieSchema.Timestamp nonNullType = (HoodieSchema.Timestamp) valueSchema.getNonNullType();
    assertFalse(nonNullType.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, nonNullType.getPrecision());
  }

  @Test
  public void testComplexSchemaMultiLevelNesting() {
    // Create a complex schema with nested records, arrays, and maps
    HoodieSchema innerRecordRequested = HoodieSchema.createRecord("Inner", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));

    HoodieSchema innerRecordTable = HoodieSchema.createRecord("Inner", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp",
            HoodieSchema.createLocalTimestampMillis(), null, null)));

    HoodieSchema requestedSchema = HoodieSchema.createRecord("Outer", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("records", HoodieSchema.createArray(innerRecordRequested), null, null),
        HoodieSchemaField.of("mapping", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG)), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("Outer", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("records", HoodieSchema.createArray(innerRecordTable), null, null),
        HoodieSchemaField.of("mapping", HoodieSchema.createMap(
            HoodieSchema.createLocalTimestampMicros()), null, null)
    ));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new complex schema");

    // Verify id field unchanged - should be same schema instance
    assertSame(requestedSchema.getField("id").get().schema(), result.getField("id").get().schema());

    // Verify array of records was repaired
    HoodieSchema arrayElementSchema = result.getField("records").get().schema().getElementType();
    HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) arrayElementSchema.getField("timestamp").get().getNonNullSchema();
    assertFalse(timestampSchema.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, timestampSchema.getPrecision());

    // Verify map values were repaired
    HoodieSchema.Timestamp mapValueSchema = (HoodieSchema.Timestamp) result.getField("mapping").get().schema().getValueType();
    assertFalse(mapValueSchema.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MICROS, mapValueSchema.getPrecision());
  }

  @Test
  public void testRepairRecordMissingFieldInTableSchema() {
    // Requested schema has a field not present in table schema
    HoodieSchema requestedSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("newField", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null)));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should return original schema unchanged since newField doesn't exist in table schema
    assertSame(requestedSchema, result, "Should return original when field missing in table schema");
  }

  @Test
  public void testRepairRecordMultipleFieldsMissingInTableSchema() {
    // Requested schema has multiple fields not present in table schema
    HoodieSchema requestedSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("newField1", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("newField2", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should return original schema unchanged since new fields don't exist in table schema
    assertSame(requestedSchema, result, "Should return original when multiple fields missing in table schema");
  }

  @Test
  public void testRepairRecordMixedMissingAndRepairableFields() {
    // Requested schema has some fields missing in table, some needing repair, some unchanged
    HoodieSchema requestedSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("newField", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should create new schema with timestamp repaired, but newField preserved from requested
    assertNotSame(requestedSchema, result, "Should create new schema");
    assertEquals(4, result.getFields().size());

    // Verify id field unchanged
    assertSame(requestedSchema.getField("id").get().schema(), result.getField("id").get().schema());

    // Verify timestamp field repaired
    HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) result.getField("timestamp").get().getNonNullSchema();
    assertFalse(timestampSchema.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, timestampSchema.getPrecision());

    // Verify newField preserved from requested schema (not in table)
    assertSame(requestedSchema.getField("newField").get().schema(), result.getField("newField").get().schema());

    // Verify name field unchanged
    assertSame(requestedSchema.getField("name").get().schema(), result.getField("name").get().schema());
  }

  @Test
  public void testRepairNestedRecordFieldMissingInTableSchema() {
    // Requested nested record has a field not present in table's nested record
    HoodieSchema nestedRequestedSchema = HoodieSchema.createRecord("NestedRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("extraField", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    HoodieSchema nestedTableSchema = HoodieSchema.createRecord("NestedRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp",
            HoodieSchema.createLocalTimestampMillis(), null, null)));

    HoodieSchema requestedSchema = HoodieSchema.createRecord("OuterRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("nested", nestedRequestedSchema, null, null)));

    HoodieSchema tableSchema = HoodieSchema.createRecord("OuterRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("nested", nestedTableSchema, null, null)));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new schema");

    // Verify id field unchanged
    assertSame(requestedSchema.getField("id").get().schema(), result.getField("id").get().schema());

    // Verify nested record was repaired but still has extraField
    HoodieSchema nestedResult = result.getField("nested").get().schema();
    assertEquals(HoodieSchemaType.RECORD, nestedResult.getType());
    assertEquals(2, nestedResult.getFields().size());

    // Timestamp should be repaired
    HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) nestedResult.getField("timestamp").get().getNonNullSchema();
    assertFalse(timestampSchema.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, timestampSchema.getPrecision());

    // extraField should be preserved from requested schema
    assertSame(nestedRequestedSchema.getField("extraField").get().schema(),
        nestedResult.getField("extraField").get().schema());
  }

  @Test
  public void testRepairRecordWholeNestedRecordMissingInTableSchema() {
    // Requested schema has a nested record field that doesn't exist in table schema
    HoodieSchema nestedRequestedSchema = HoodieSchema.createRecord("NestedRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));

    HoodieSchema requestedSchema = HoodieSchema.createRecord("OuterRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("newNested", nestedRequestedSchema, null, null)));

    HoodieSchema tableSchema = HoodieSchema.createRecord("OuterRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null)));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should return original schema unchanged since newNested field doesn't exist in table
    assertSame(requestedSchema, result, "Should return original when nested field missing in table schema");
  }

  @Test
  public void testRepairRecordPreservesFieldMetadata() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("TestRecord", null, "Test documentation",
        Collections.singletonList(HoodieSchemaField.of("timestamp",
            HoodieSchema.create(HoodieSchemaType.LONG), "Timestamp field", null)));

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp",
            HoodieSchema.createLocalTimestampMillis(), null, null)));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    assertEquals("TestRecord", result.getName());
    assertEquals("Test documentation", result.getDoc().get());
    assertEquals("Timestamp field", result.getField("timestamp").get().doc().get());
  }

  @Test
  public void testEdgeCaseEmptyRecord() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("EmptyRecord", null, null, Collections.emptyList());
    HoodieSchema tableSchema = HoodieSchema.createRecord("EmptyRecord", null, null, Collections.emptyList());

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Empty records should return same instance");
  }

  @Test
  public void testRepairRecordFirstFieldChanged() {
    // Test the optimization path where the first field needs repair
    HoodieSchema requestedSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("timestamp1", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
            HoodieSchemaField.of("timestamp2", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
        ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("timestamp1", HoodieSchema.createLocalTimestampMillis(), null, null),
            HoodieSchemaField.of("timestamp2", HoodieSchema.createLocalTimestampMicros(), null, null)
        ));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    HoodieSchema.Timestamp ts1 = (HoodieSchema.Timestamp) result.getField("timestamp1").get().getNonNullSchema();
    assertFalse(ts1.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, ts1.getPrecision());
    HoodieSchema.Timestamp ts2 = (HoodieSchema.Timestamp) result.getField("timestamp2").get().getNonNullSchema();
    assertFalse(ts2.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MICROS, ts2.getPrecision());
  }

  @Test
  public void testRepairRecordLastFieldChanged() {
    // Test the optimization path where only the last field needs repair
    HoodieSchema requestedSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
            HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
        ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
            HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)
        ));

    HoodieSchema result = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    // Verify id and name fields unchanged - should be same schema instances
    assertSame(requestedSchema.getField("id").get().schema(), result.getField("id").get().schema());
    assertSame(requestedSchema.getField("name").get().schema(), result.getField("name").get().schema());
    // Verify timestamp field repaired
    HoodieSchema.Timestamp timestampSchema = (HoodieSchema.Timestamp) result.getField("timestamp").get().getNonNullSchema();
    assertFalse(timestampSchema.isUtcAdjusted());
    assertEquals(HoodieSchema.TimePrecision.MILLIS, timestampSchema.getPrecision());
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createTimestampMillis();
    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for LONG with timestamp-millis logical type");
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithoutLogicalType() {
    HoodieSchema schema = HoodieSchema.create(HoodieSchemaType.LONG);
    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for LONG without logical type");
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithTimestampMicros() {
    HoodieSchema schema = HoodieSchema.createTimestampMicros();
    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for LONG with timestamp-micros logical type");
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithLocalTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createLocalTimestampMillis();
    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for LONG with local-timestamp-millis logical type");
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithLocalTimestampMicros() {
    HoodieSchema schema = HoodieSchema.createLocalTimestampMicros();
    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for LONG with local-timestamp-micros logical type");
  }

  @Test
  public void testHasTimestampMillisFieldOtherPrimitiveTypes() {
    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(HoodieSchema.create(HoodieSchemaType.STRING)),
        "Should return false for STRING type");
    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(HoodieSchema.create(HoodieSchemaType.INT)),
        "Should return false for INT type");
    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(HoodieSchema.create(HoodieSchemaType.FLOAT)),
        "Should return false for FLOAT type");
    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(HoodieSchema.create(HoodieSchemaType.DOUBLE)),
        "Should return false for DOUBLE type");
    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
        "Should return false for BOOLEAN type");
  }

  @Test
  public void testHasTimestampMillisFieldRecordWithTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for record containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldRecordWithoutTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMicros(), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for record without timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldRecordEmpty() {
    HoodieSchema schema = HoodieSchema.createRecord("EmptyRecord", null, null, Collections.emptyList());

    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for empty record");
  }

  @Test
  public void testHasTimestampMillisFieldNestedRecord() {
    HoodieSchema innerSchema = HoodieSchema.createRecord("InnerRecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)
    ));

    HoodieSchema outerSchema = HoodieSchema.createRecord("OuterRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("inner", innerSchema, null, null)));

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(outerSchema),
        "Should return true for nested record containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldDeeplyNestedRecord() {
    HoodieSchema level3 = HoodieSchema.createRecord("Level3", null, null,
        Collections.singletonList(HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)));

    HoodieSchema level2 = HoodieSchema.createRecord("Level2", null, null,
        Collections.singletonList(HoodieSchemaField.of("data", level3, null, null)));

    HoodieSchema level1 = HoodieSchema.createRecord("Level1", null, null,
        Collections.singletonList(HoodieSchemaField.of("nested", level2, null, null)));

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(level1),
        "Should return true for deeply nested record containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldArrayWithTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createArray(HoodieSchema.createTimestampMillis());

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for array with timestamp-millis elements");
  }

  @Test
  public void testHasTimestampMillisFieldArrayWithoutTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));

    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for array without timestamp-millis elements");
  }

  @Test
  public void testHasTimestampMillisFieldArrayOfRecordsWithTimestampMillis() {
    HoodieSchema elementSchema = HoodieSchema.createRecord("Element", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)
    ));

    HoodieSchema schema = HoodieSchema.createArray(elementSchema);

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for array of records containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldMapWithTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createMap(HoodieSchema.createTimestampMillis());

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for map with timestamp-millis values");
  }

  @Test
  public void testHasTimestampMillisFieldMapWithoutTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING));

    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for map without timestamp-millis values");
  }

  @Test
  public void testHasTimestampMillisFieldMapOfRecordsWithTimestampMillis() {
    HoodieSchema valueSchema = HoodieSchema.createRecord("Value", null, null, Arrays.asList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)
    ));

    HoodieSchema schema = HoodieSchema.createMap(valueSchema);

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for map of records containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldUnionWithTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createNullable(HoodieSchema.createTimestampMillis());

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for nullable union with timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldUnionWithoutTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createUnion(
        HoodieSchema.create(HoodieSchemaType.NULL),
        HoodieSchema.create(HoodieSchemaType.LONG)
    );

    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for nullable union without timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldUnionWithRecordContainingTimestampMillis() {
    HoodieSchema recordSchema = HoodieSchema.createRecord("Record", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)
    ));

    HoodieSchema schema = HoodieSchema.createUnion(
        HoodieSchema.create(HoodieSchemaType.NULL),
        recordSchema
    );

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for nullable union with record containing timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldComplexNestedStructure() {
    // Create a complex schema with arrays, maps, and nested records
    HoodieSchema innerRecordSchema = HoodieSchema.createRecord("InnerRecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)
    ));

    HoodieSchema complexSchema = HoodieSchema.createRecord("ComplexRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("arrayOfRecords", HoodieSchema.createArray(innerRecordSchema), null, null),
        HoodieSchemaField.of("mapOfStrings", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING)), null, null)
    ));

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(complexSchema),
        "Should return true for complex nested structure containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldComplexStructureWithoutTimestampMillis() {
    HoodieSchema innerRecordSchema = HoodieSchema.createRecord("InnerRecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema complexSchema = HoodieSchema.createRecord("ComplexRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("arrayOfRecords", HoodieSchema.createArray(innerRecordSchema), null, null),
        HoodieSchemaField.of("mapOfLongs", HoodieSchema.createMap(HoodieSchema.createTimestampMicros()), null, null)
    ));

    assertFalse(HoodieSchemaRepair.hasTimestampMillisField(complexSchema),
        "Should return false for complex structure without timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldFirstFieldMatches() {
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null),
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true when first field is timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldLastFieldMatches() {
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)
    ));

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true when last field is timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldMultipleTimestampMillisFields() {
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("createdAt", HoodieSchema.createTimestampMillis(), null, null),
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("updatedAt", HoodieSchema.createTimestampMillis(), null, null)
    ));

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true when multiple timestamp-millis fields exist");
  }

  @Test
  public void testHasTimestampMillisFieldNullableFieldWithTimestampMillis() {
    HoodieSchema schema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createNullable(HoodieSchema.createTimestampMillis()))));

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for nullable field with timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldArrayOfNullableTimestampMillis() {
    HoodieSchema elementSchema = HoodieSchema.createNullable(HoodieSchema.createTimestampMillis());

    HoodieSchema schema = HoodieSchema.createArray(elementSchema);

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for array of nullable timestamp-millis elements");
  }

  @Test
  public void testHasTimestampMillisFieldMapOfNullableTimestampMillis() {
    HoodieSchema valueSchema = HoodieSchema.createNullable(HoodieSchema.createTimestampMillis());

    HoodieSchema schema = HoodieSchema.createMap(valueSchema);

    assertTrue(HoodieSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for map of nullable timestamp-millis values");
  }
}
