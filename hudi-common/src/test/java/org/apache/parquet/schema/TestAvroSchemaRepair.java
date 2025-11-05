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

import org.apache.hudi.avro.AvroSchemaUtils;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link AvroSchemaRepair}.
 */
public class TestAvroSchemaRepair {

  @Test
  public void testNoRepairNeededIdenticalSchemas() {
    Schema requestedSchema = Schema.create(Schema.Type.LONG);
    Schema tableSchema = Schema.create(Schema.Type.LONG);

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "When schemas are identical, should return same instance");
  }

  @Test
  public void testNoRepairNeededDifferentPrimitiveTypes() {
    Schema requestedSchema = Schema.create(Schema.Type.STRING);
    Schema tableSchema = Schema.create(Schema.Type.INT);

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "When types differ, should return original schema");
  }

  @Test
  public void testRepairLongWithoutLogicalTypeToLocalTimestampMillis() {
    Schema requestedSchema = Schema.create(Schema.Type.LONG);
    Schema tableSchema = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create a new schema with logical type");
    assertEquals(Schema.Type.LONG, result.getType());
    assertEquals(LogicalTypes.localTimestampMillis(), result.getLogicalType());
  }

  @Test
  public void testRepairLongWithoutLogicalTypeToLocalTimestampMicros() {
    Schema requestedSchema = Schema.create(Schema.Type.LONG);
    Schema tableSchema = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create a new schema with logical type");
    assertEquals(Schema.Type.LONG, result.getType());
    assertEquals(LogicalTypes.localTimestampMicros(), result.getLogicalType());
  }

  @Test
  public void testRepairTimestampMicrosToTimestampMillis() {
    Schema requestedSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema tableSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create a new schema with timestamp-millis");
    assertEquals(Schema.Type.LONG, result.getType());
    assertEquals(LogicalTypes.timestampMillis(), result.getLogicalType());
  }

  @Test
  public void testNoRepairNeededTimestampMillisToTimestampMicros() {
    // This direction should NOT trigger repair
    Schema requestedSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema tableSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should not repair timestamp-millis to timestamp-micros");
  }

  @Test
  public void testNoRepairNeededNonLongTypes() {
    Schema requestedSchema = Schema.create(Schema.Type.INT);
    Schema tableSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should not repair non-LONG types");
  }

  @Test
  public void testRepairNullableSchemaLongToLocalTimestampMillis() {
    Schema requestedSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.LONG)
    );
    Schema tableSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    );

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new nullable schema with repaired type");
    assertEquals(Schema.Type.UNION, result.getType());
    assertEquals(2, result.getTypes().size());

    Schema nonNullType = AvroSchemaUtils.getNonNullTypeFromUnion(result);
    assertEquals(LogicalTypes.localTimestampMillis(), nonNullType.getLogicalType());
  }

  @Test
  public void testRepairNullableSchemaTimestampMicrosToMillis() {
    Schema requestedSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
    );
    Schema tableSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    );

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new nullable schema");
    assertEquals(Schema.Type.UNION, result.getType());

    Schema nonNullType = AvroSchemaUtils.getNonNullTypeFromUnion(result);
    assertEquals(LogicalTypes.timestampMillis(), nonNullType.getLogicalType());
  }

  @Test
  public void testRepairRecordSingleField() {
    Schema requestedSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("timestamp").type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new record schema");
    assertEquals(Schema.Type.RECORD, result.getType());
    assertEquals("TestRecord", result.getName());
    assertEquals(1, result.getFields().size());

    Schema.Field field = result.getField("timestamp");
    assertEquals(LogicalTypes.localTimestampMillis(), field.schema().getLogicalType());
  }

  @Test
  public void testRepairRecordMultipleFieldsOnlyOneNeedsRepair() {
    Schema requestedSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type().longType().noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new record schema");
    assertEquals(3, result.getFields().size());

    // Verify id field unchanged - should be same schema instance
    assertSame(requestedSchema.getField("id").schema(), result.getField("id").schema());

    // Verify timestamp field repaired
    assertEquals(LogicalTypes.localTimestampMicros(), result.getField("timestamp").schema().getLogicalType());

    // Verify name field unchanged - should be same schema instance
    assertSame(requestedSchema.getField("name").schema(), result.getField("name").schema());
  }

  @Test
  public void testRepairRecordNestedRecord() {
    Schema nestedRequestedSchema = SchemaBuilder.record("NestedRecord")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .endRecord();

    Schema nestedTableSchema = SchemaBuilder.record("NestedRecord")
        .fields()
        .name("timestamp").type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema requestedSchema = SchemaBuilder.record("OuterRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("nested").type(nestedRequestedSchema).noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("OuterRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("nested").type(nestedTableSchema).noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new schema for nested record");

    // Verify id field unchanged - should be same schema instance
    assertSame(requestedSchema.getField("id").schema(), result.getField("id").schema());

    // Verify nested record was repaired
    Schema nestedResult = result.getField("nested").schema();
    assertEquals(Schema.Type.RECORD, nestedResult.getType());
    assertEquals(LogicalTypes.localTimestampMillis(),
        nestedResult.getField("timestamp").schema().getLogicalType());
  }

  @Test
  public void testRepairRecordNullableNestedField() {
    Schema requestedSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("timestamp").type().optional().longType()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("timestamp").type().optional().type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new schema");

    Schema fieldSchema = result.getField("timestamp").schema();
    assertEquals(Schema.Type.UNION, fieldSchema.getType());

    Schema nonNullType = AvroSchemaUtils.getNonNullTypeFromUnion(fieldSchema);
    assertEquals(LogicalTypes.localTimestampMillis(), nonNullType.getLogicalType());
  }

  @Test
  public void testRepairArrayElementNeedsRepair() {
    Schema requestedSchema = Schema.createArray(Schema.create(Schema.Type.LONG));
    Schema tableSchema = Schema.createArray(
        LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    );

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new array schema");
    assertEquals(Schema.Type.ARRAY, result.getType());
    assertEquals(LogicalTypes.localTimestampMillis(), result.getElementType().getLogicalType());
  }

  @Test
  public void testRepairArrayNoRepairNeeded() {
    Schema elementSchema = Schema.create(Schema.Type.STRING);
    Schema requestedSchema = Schema.createArray(elementSchema);
    Schema tableSchema = Schema.createArray(elementSchema);

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should return same array when no repair needed");
  }

  @Test
  public void testRepairArrayNullableElements() {
    Schema requestedSchema = Schema.createArray(
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG))
    );
    Schema tableSchema = Schema.createArray(
        Schema.createUnion(
            Schema.create(Schema.Type.NULL),
            LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
        )
    );

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new array schema");
    Schema elementSchema = result.getElementType();
    assertEquals(Schema.Type.UNION, elementSchema.getType());

    Schema nonNullType = AvroSchemaUtils.getNonNullTypeFromUnion(elementSchema);
    assertEquals(LogicalTypes.localTimestampMicros(), nonNullType.getLogicalType());
  }

  @Test
  public void testRepairMapValueNeedsRepair() {
    Schema requestedSchema = Schema.createMap(Schema.create(Schema.Type.LONG));
    Schema tableSchema = Schema.createMap(
        LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    );

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new map schema");
    assertEquals(Schema.Type.MAP, result.getType());
    assertEquals(LogicalTypes.localTimestampMillis(), result.getValueType().getLogicalType());
  }

  @Test
  public void testRepairMapNoRepairNeeded() {
    Schema valueSchema = Schema.create(Schema.Type.STRING);
    Schema requestedSchema = Schema.createMap(valueSchema);
    Schema tableSchema = Schema.createMap(valueSchema);

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should return same map when no repair needed");
  }

  @Test
  public void testRepairMapNullableValues() {
    Schema requestedSchema = Schema.createMap(
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG))
    );
    Schema tableSchema = Schema.createMap(
        Schema.createUnion(
            Schema.create(Schema.Type.NULL),
            LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
        )
    );

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new map schema");
    Schema valueSchema = result.getValueType();
    assertEquals(Schema.Type.UNION, valueSchema.getType());

    Schema nonNullType = AvroSchemaUtils.getNonNullTypeFromUnion(valueSchema);
    assertEquals(LogicalTypes.localTimestampMillis(), nonNullType.getLogicalType());
  }

  @Test
  public void testComplexSchemaMultiLevelNesting() {
    // Create a complex schema with nested records, arrays, and maps
    Schema innerRecordRequested = SchemaBuilder.record("Inner")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .endRecord();

    Schema innerRecordTable = SchemaBuilder.record("Inner")
        .fields()
        .name("timestamp").type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema requestedSchema = SchemaBuilder.record("Outer")
        .fields()
        .name("id").type().intType().noDefault()
        .name("records").type().array().items(innerRecordRequested).noDefault()
        .name("mapping").type().map().values(Schema.create(Schema.Type.LONG)).noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("Outer")
        .fields()
        .name("id").type().intType().noDefault()
        .name("records").type().array().items(innerRecordTable).noDefault()
        .name("mapping").type().map().values(
            LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
        ).noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new complex schema");

    // Verify id field unchanged - should be same schema instance
    assertSame(requestedSchema.getField("id").schema(), result.getField("id").schema());

    // Verify array of records was repaired
    Schema arrayElementSchema = result.getField("records").schema().getElementType();
    assertEquals(LogicalTypes.localTimestampMillis(),
        arrayElementSchema.getField("timestamp").schema().getLogicalType());

    // Verify map values were repaired
    Schema mapValueSchema = result.getField("mapping").schema().getValueType();
    assertEquals(LogicalTypes.localTimestampMicros(), mapValueSchema.getLogicalType());
  }

  @Test
  public void testRepairRecordMissingFieldInTableSchema() {
    // Requested schema has a field not present in table schema
    Schema requestedSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("newField").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should return original schema unchanged since newField doesn't exist in table schema
    assertSame(requestedSchema, result, "Should return original when field missing in table schema");
  }

  @Test
  public void testRepairRecordMultipleFieldsMissingInTableSchema() {
    // Requested schema has multiple fields not present in table schema
    Schema requestedSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("newField1").type().longType().noDefault()
        .name("name").type().stringType().noDefault()
        .name("newField2").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should return original schema unchanged since new fields don't exist in table schema
    assertSame(requestedSchema, result, "Should return original when multiple fields missing in table schema");
  }

  @Test
  public void testRepairRecordMixedMissingAndRepairableFields() {
    // Requested schema has some fields missing in table, some needing repair, some unchanged
    Schema requestedSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type().longType().noDefault()
        .name("newField").type().longType().noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should create new schema with timestamp repaired, but newField preserved from requested
    assertNotSame(requestedSchema, result, "Should create new schema");
    assertEquals(4, result.getFields().size());

    // Verify id field unchanged
    assertSame(requestedSchema.getField("id").schema(), result.getField("id").schema());

    // Verify timestamp field repaired
    assertEquals(LogicalTypes.localTimestampMillis(), result.getField("timestamp").schema().getLogicalType());

    // Verify newField preserved from requested schema (not in table)
    assertSame(requestedSchema.getField("newField").schema(), result.getField("newField").schema());

    // Verify name field unchanged
    assertSame(requestedSchema.getField("name").schema(), result.getField("name").schema());
  }

  @Test
  public void testRepairNestedRecordFieldMissingInTableSchema() {
    // Requested nested record has a field not present in table's nested record
    Schema nestedRequestedSchema = SchemaBuilder.record("NestedRecord")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .name("extraField").type().stringType().noDefault()
        .endRecord();

    Schema nestedTableSchema = SchemaBuilder.record("NestedRecord")
        .fields()
        .name("timestamp").type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema requestedSchema = SchemaBuilder.record("OuterRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("nested").type(nestedRequestedSchema).noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("OuterRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("nested").type(nestedTableSchema).noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new schema");

    // Verify id field unchanged
    assertSame(requestedSchema.getField("id").schema(), result.getField("id").schema());

    // Verify nested record was repaired but still has extraField
    Schema nestedResult = result.getField("nested").schema();
    assertEquals(Schema.Type.RECORD, nestedResult.getType());
    assertEquals(2, nestedResult.getFields().size());

    // Timestamp should be repaired
    assertEquals(LogicalTypes.localTimestampMillis(),
        nestedResult.getField("timestamp").schema().getLogicalType());

    // extraField should be preserved from requested schema
    assertSame(nestedRequestedSchema.getField("extraField").schema(),
        nestedResult.getField("extraField").schema());
  }

  @Test
  public void testRepairRecordWholeNestedRecordMissingInTableSchema() {
    // Requested schema has a nested record field that doesn't exist in table schema
    Schema nestedRequestedSchema = SchemaBuilder.record("NestedRecord")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .endRecord();

    Schema requestedSchema = SchemaBuilder.record("OuterRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("newNested").type(nestedRequestedSchema).noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("OuterRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should return original schema unchanged since newNested field doesn't exist in table
    assertSame(requestedSchema, result, "Should return original when nested field missing in table schema");
  }

  @Test
  public void testRepairRecordPreservesFieldMetadata() {
    Schema requestedSchema = SchemaBuilder.record("TestRecord")
        .doc("Test documentation")
        .fields()
        .name("timestamp").doc("Timestamp field").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("timestamp").type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    assertEquals("TestRecord", result.getName());
    assertEquals("Test documentation", result.getDoc());
    assertEquals("Timestamp field", result.getField("timestamp").doc());
  }

  @Test
  public void testEdgeCaseEmptyRecord() {
    Schema requestedSchema = SchemaBuilder.record("EmptyRecord").fields().endRecord();
    Schema tableSchema = SchemaBuilder.record("EmptyRecord").fields().endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Empty records should return same instance");
  }

  @Test
  public void testRepairRecordFirstFieldChanged() {
    // Test the optimization path where the first field needs repair
    Schema requestedSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("timestamp1").type().longType().noDefault()
        .name("timestamp2").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("timestamp1").type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .name("timestamp2").type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    assertEquals(LogicalTypes.localTimestampMillis(), result.getField("timestamp1").schema().getLogicalType());
    assertEquals(LogicalTypes.localTimestampMicros(), result.getField("timestamp2").schema().getLogicalType());
  }

  @Test
  public void testRepairRecordLastFieldChanged() {
    // Test the optimization path where only the last field needs repair
    Schema requestedSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("name").type().stringType().noDefault()
        .name("timestamp").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("name").type().stringType().noDefault()
        .name("timestamp").type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema result = AvroSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    // Verify id and name fields unchanged - should be same schema instances
    assertSame(requestedSchema.getField("id").schema(), result.getField("id").schema());
    assertSame(requestedSchema.getField("name").schema(), result.getField("name").schema());
    // Verify timestamp field repaired
    assertEquals(LogicalTypes.localTimestampMillis(), result.getField("timestamp").schema().getLogicalType());
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithTimestampMillis() {
    Schema schema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for LONG with timestamp-millis logical type");
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithoutLogicalType() {
    Schema schema = Schema.create(Schema.Type.LONG);
    assertFalse(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for LONG without logical type");
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithTimestampMicros() {
    Schema schema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    assertFalse(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for LONG with timestamp-micros logical type");
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithLocalTimestampMillis() {
    Schema schema = LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for LONG with local-timestamp-millis logical type");
  }

  @Test
  public void testHasTimestampMillisFieldPrimitiveLongWithLocalTimestampMicros() {
    Schema schema = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    assertFalse(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for LONG with local-timestamp-micros logical type");
  }

  @Test
  public void testHasTimestampMillisFieldOtherPrimitiveTypes() {
    assertFalse(AvroSchemaRepair.hasTimestampMillisField(Schema.create(Schema.Type.STRING)),
        "Should return false for STRING type");
    assertFalse(AvroSchemaRepair.hasTimestampMillisField(Schema.create(Schema.Type.INT)),
        "Should return false for INT type");
    assertFalse(AvroSchemaRepair.hasTimestampMillisField(Schema.create(Schema.Type.FLOAT)),
        "Should return false for FLOAT type");
    assertFalse(AvroSchemaRepair.hasTimestampMillisField(Schema.create(Schema.Type.DOUBLE)),
        "Should return false for DOUBLE type");
    assertFalse(AvroSchemaRepair.hasTimestampMillisField(Schema.create(Schema.Type.BOOLEAN)),
        "Should return false for BOOLEAN type");
  }

  @Test
  public void testHasTimestampMillisFieldRecordWithTimestampMillis() {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for record containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldRecordWithoutTimestampMillis() {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    assertFalse(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for record without timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldRecordEmpty() {
    Schema schema = SchemaBuilder.record("EmptyRecord").fields().endRecord();

    assertFalse(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for empty record");
  }

  @Test
  public void testHasTimestampMillisFieldNestedRecord() {
    Schema innerSchema = SchemaBuilder.record("InnerRecord")
        .fields()
        .name("timestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema outerSchema = SchemaBuilder.record("OuterRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("inner").type(innerSchema).noDefault()
        .endRecord();

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(outerSchema),
        "Should return true for nested record containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldDeeplyNestedRecord() {
    Schema level3 = SchemaBuilder.record("Level3")
        .fields()
        .name("timestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema level2 = SchemaBuilder.record("Level2")
        .fields()
        .name("data").type(level3).noDefault()
        .endRecord();

    Schema level1 = SchemaBuilder.record("Level1")
        .fields()
        .name("nested").type(level2).noDefault()
        .endRecord();

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(level1),
        "Should return true for deeply nested record containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldArrayWithTimestampMillis() {
    Schema schema = Schema.createArray(
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    );

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for array with timestamp-millis elements");
  }

  @Test
  public void testHasTimestampMillisFieldArrayWithoutTimestampMillis() {
    Schema schema = Schema.createArray(Schema.create(Schema.Type.STRING));

    assertFalse(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for array without timestamp-millis elements");
  }

  @Test
  public void testHasTimestampMillisFieldArrayOfRecordsWithTimestampMillis() {
    Schema elementSchema = SchemaBuilder.record("Element")
        .fields()
        .name("timestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema schema = Schema.createArray(elementSchema);

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for array of records containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldMapWithTimestampMillis() {
    Schema schema = Schema.createMap(
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    );

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for map with timestamp-millis values");
  }

  @Test
  public void testHasTimestampMillisFieldMapWithoutTimestampMillis() {
    Schema schema = Schema.createMap(Schema.create(Schema.Type.STRING));

    assertFalse(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for map without timestamp-millis values");
  }

  @Test
  public void testHasTimestampMillisFieldMapOfRecordsWithTimestampMillis() {
    Schema valueSchema = SchemaBuilder.record("Value")
        .fields()
        .name("timestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema schema = Schema.createMap(valueSchema);

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for map of records containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldUnionWithTimestampMillis() {
    Schema schema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    );

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for nullable union with timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldUnionWithoutTimestampMillis() {
    Schema schema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.LONG)
    );

    assertFalse(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return false for nullable union without timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldUnionWithRecordContainingTimestampMillis() {
    Schema recordSchema = SchemaBuilder.record("Record")
        .fields()
        .name("timestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema schema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        recordSchema
    );

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for nullable union with record containing timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldComplexNestedStructure() {
    // Create a complex schema with arrays, maps, and nested records
    Schema innerRecordSchema = SchemaBuilder.record("InnerRecord")
        .fields()
        .name("timestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    Schema complexSchema = SchemaBuilder.record("ComplexRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("arrayOfRecords").type().array().items(innerRecordSchema).noDefault()
        .name("mapOfStrings").type().map().values().stringType().noDefault()
        .endRecord();

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(complexSchema),
        "Should return true for complex nested structure containing timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldComplexStructureWithoutTimestampMillis() {
    Schema innerRecordSchema = SchemaBuilder.record("InnerRecord")
        .fields()
        .name("value").type().longType().noDefault()
        .endRecord();

    Schema complexSchema = SchemaBuilder.record("ComplexRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("arrayOfRecords").type().array().items(innerRecordSchema).noDefault()
        .name("mapOfLongs").type().map().values(
            LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))
        ).noDefault()
        .endRecord();

    assertFalse(AvroSchemaRepair.hasTimestampMillisField(complexSchema),
        "Should return false for complex structure without timestamp-millis field");
  }

  @Test
  public void testHasTimestampMillisFieldFirstFieldMatches() {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("timestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .name("id").type().intType().noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true when first field is timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldLastFieldMatches() {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("name").type().stringType().noDefault()
        .name("timestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true when last field is timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldMultipleTimestampMillisFields() {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("createdAt").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .name("id").type().intType().noDefault()
        .name("updatedAt").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .endRecord();

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true when multiple timestamp-millis fields exist");
  }

  @Test
  public void testHasTimestampMillisFieldNullableFieldWithTimestampMillis() {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type().optional().type(
            LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
        )
        .endRecord();

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for nullable field with timestamp-millis");
  }

  @Test
  public void testHasTimestampMillisFieldArrayOfNullableTimestampMillis() {
    Schema elementSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    );

    Schema schema = Schema.createArray(elementSchema);

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for array of nullable timestamp-millis elements");
  }

  @Test
  public void testHasTimestampMillisFieldMapOfNullableTimestampMillis() {
    Schema valueSchema = Schema.createUnion(
        Schema.create(Schema.Type.NULL),
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))
    );

    Schema schema = Schema.createMap(valueSchema);

    assertTrue(AvroSchemaRepair.hasTimestampMillisField(schema),
        "Should return true for map of nullable timestamp-millis values");
  }
}
