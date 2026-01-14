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

import org.apache.hudi.common.util.Option;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests {@link SchemaRepair}.
 */
public class TestSchemaRepair {

  @Test
  public void testNoRepairNeededIdenticalSchemas() {
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );
    MessageType tableSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "When schemas are identical, should return same instance");
  }

  @Test
  public void testNoRepairNeededDifferentPrimitiveTypes() {
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
    );
    MessageType tableSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "When field names differ, should return original schema");
  }

  @Test
  public void testRepairLongWithoutLogicalTypeToLocalTimestampMillis() {
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );
    MessageType tableSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create a new schema with logical type");
    PrimitiveType timestampField = result.getType("timestamp").asPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64, timestampField.getPrimitiveTypeName());
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        timestampField.getLogicalTypeAnnotation());
  }

  @Test
  public void testRepairLongWithoutLogicalTypeToLocalTimestampMicros() {
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );
    MessageType tableSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create a new schema with logical type");
    PrimitiveType timestampField = result.getType("timestamp").asPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64, timestampField.getPrimitiveTypeName());
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS),
        timestampField.getLogicalTypeAnnotation());
  }

  @Test
  public void testRepairTimestampMicrosToTimestampMillis() {
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("timestamp")
    );
    MessageType tableSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create a new schema with timestamp-millis");
    PrimitiveType timestampField = result.getType("timestamp").asPrimitiveType();
    assertEquals(PrimitiveType.PrimitiveTypeName.INT64, timestampField.getPrimitiveTypeName());
    assertEquals(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS),
        timestampField.getLogicalTypeAnnotation());
  }

  @Test
  public void testNoRepairNeededTimestampMillisToTimestampMicros() {
    // This direction should NOT trigger repair
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );
    MessageType tableSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should not repair timestamp-millis to timestamp-micros");
  }

  @Test
  public void testNoRepairNeededNonLongTypes() {
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id")
    );
    MessageType tableSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.dateType())
            .named("id")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Should not repair non-LONG types");
  }

  @Test
  public void testRepairRecordSingleField() {
    MessageType requestedSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );

    MessageType tableSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new record schema");
    assertEquals(1, result.getFields().size());

    PrimitiveType field = result.getType("timestamp").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        field.getLogicalTypeAnnotation());
  }

  @Test
  public void testRepairRecordMultipleFieldsOnlyOneNeedsRepair() {
    MessageType requestedSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
    );

    MessageType tableSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("timestamp"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new record schema");
    assertEquals(3, result.getFields().size());

    // Verify id field unchanged - should be same type instance
    assertSame(requestedSchema.getType("id"), result.getType("id"));

    // Verify timestamp field repaired
    PrimitiveType timestampField = result.getType("timestamp").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS),
        timestampField.getLogicalTypeAnnotation());

    // Verify name field unchanged - should be same type instance
    assertSame(requestedSchema.getType("name"), result.getType("name"));
  }

  @Test
  public void testRepairRecordNestedRecord() {
    GroupType nestedRequestedSchema = new GroupType(Type.Repetition.REQUIRED, "nested",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );

    GroupType nestedTableSchema = new GroupType(Type.Repetition.REQUIRED, "nested",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );

    MessageType requestedSchema = new MessageType("OuterRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        nestedRequestedSchema
    );

    MessageType tableSchema = new MessageType("OuterRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        nestedTableSchema
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new schema for nested record");

    // Verify id field unchanged - should be same type instance
    assertSame(requestedSchema.getType("id"), result.getType("id"));

    // Verify nested record was repaired
    GroupType nestedResult = result.getType("nested").asGroupType();
    PrimitiveType nestedTimestamp = nestedResult.getType("timestamp").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        nestedTimestamp.getLogicalTypeAnnotation());
  }

  @Test
  public void testRepairRecordMissingFieldInTableSchema() {
    // Requested schema has a field not present in table schema
    MessageType requestedSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("newField")
    );

    MessageType tableSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should return original schema unchanged since newField doesn't exist in table schema
    assertSame(requestedSchema, result, "Should return original when field missing in table schema");
  }

  @Test
  public void testRepairRecordMultipleFieldsMissingInTableSchema() {
    // Requested schema has multiple fields not present in table schema
    MessageType requestedSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("newField1"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("name"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("newField2")
    );

    MessageType tableSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should return original schema unchanged since new fields don't exist in table schema
    assertSame(requestedSchema, result, "Should return original when multiple fields missing in table schema");
  }

  @Test
  public void testRepairRecordMixedMissingAndRepairableFields() {
    // Requested schema has some fields missing in table, some needing repair, some unchanged
    MessageType requestedSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("newField"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
    );

    MessageType tableSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should create new schema with timestamp repaired, but newField preserved from requested
    assertNotSame(requestedSchema, result, "Should create new schema");
    assertEquals(4, result.getFields().size());

    // Verify id field unchanged
    assertSame(requestedSchema.getType("id"), result.getType("id"));

    // Verify timestamp field repaired
    PrimitiveType timestampField = result.getType("timestamp").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        timestampField.getLogicalTypeAnnotation());

    // Verify newField preserved from requested schema (not in table)
    assertSame(requestedSchema.getType("newField"), result.getType("newField"));

    // Verify name field unchanged
    assertSame(requestedSchema.getType("name"), result.getType("name"));
  }

  @Test
  public void testRepairNestedRecordFieldMissingInTableSchema() {
    // Requested nested record has a field not present in table's nested record
    GroupType nestedRequestedSchema = new GroupType(Type.Repetition.REQUIRED, "nested",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("extraField")
    );

    GroupType nestedTableSchema = new GroupType(Type.Repetition.REQUIRED, "nested",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );

    MessageType requestedSchema = new MessageType("OuterRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        nestedRequestedSchema
    );

    MessageType tableSchema = new MessageType("OuterRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        nestedTableSchema
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result, "Should create new schema");

    // Verify id field unchanged
    assertSame(requestedSchema.getType("id"), result.getType("id"));

    // Verify nested record was repaired but still has extraField
    GroupType nestedResult = result.getType("nested").asGroupType();
    assertEquals(2, nestedResult.getFieldCount());

    // Timestamp should be repaired
    PrimitiveType timestampField = nestedResult.getType("timestamp").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        timestampField.getLogicalTypeAnnotation());

    // extraField should be preserved from requested schema
    assertSame(nestedRequestedSchema.getType("extraField"), nestedResult.getType("extraField"));
  }

  @Test
  public void testRepairRecordWholeNestedRecordMissingInTableSchema() {
    // Requested schema has a nested record field that doesn't exist in table schema
    GroupType nestedRequestedSchema = new GroupType(Type.Repetition.REQUIRED, "newNested",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );

    MessageType requestedSchema = new MessageType("OuterRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        nestedRequestedSchema
    );

    MessageType tableSchema = new MessageType("OuterRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Should return original schema unchanged since newNested field doesn't exist in table
    assertSame(requestedSchema, result, "Should return original when nested field missing in table schema");
  }

  @Test
  public void testEdgeCaseEmptyRecord() {
    MessageType requestedSchema = new MessageType("EmptyRecord");
    MessageType tableSchema = new MessageType("EmptyRecord");

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertSame(requestedSchema, result, "Empty records should return same instance");
  }

  @Test
  public void testRepairRecordFirstFieldChanged() {
    // Test the optimization path where the first field needs repair
    MessageType requestedSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp1"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp2")
    );

    MessageType tableSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp1"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("timestamp2")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    PrimitiveType timestamp1 = result.getType("timestamp1").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        timestamp1.getLogicalTypeAnnotation());
    PrimitiveType timestamp2 = result.getType("timestamp2").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS),
        timestamp2.getLogicalTypeAnnotation());
  }

  @Test
  public void testRepairRecordLastFieldChanged() {
    // Test the optimization path where only the last field needs repair
    MessageType requestedSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("name"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );

    MessageType tableSchema = new MessageType("TestRecord",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.REQUIRED)
            .named("id"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.stringType())
            .named("name"),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    // Verify id and name fields unchanged - should be same type instances
    assertSame(requestedSchema.getType("id"), result.getType("id"));
    assertSame(requestedSchema.getType("name"), result.getType("name"));
    // Verify timestamp field repaired
    PrimitiveType timestampField = result.getType("timestamp").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        timestampField.getLogicalTypeAnnotation());
  }

  @Test
  public void testRepairLogicalTypesWithOptionEmpty() {
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, Option.empty());

    assertSame(requestedSchema, result, "Should return original when Option is empty");
  }

  @Test
  public void testRepairLogicalTypesWithOptionPresent() {
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );
    MessageType tableSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, Option.of(tableSchema));

    assertNotSame(requestedSchema, result, "Should repair when Option is present");
    PrimitiveType timestampField = result.getType("timestamp").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        timestampField.getLogicalTypeAnnotation());
  }

  @Test
  public void testRepairOptionalFieldRepetition() {
    // Test that repair preserves the requested field's repetition (OPTIONAL vs REQUIRED)
    MessageType requestedSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
            .named("timestamp")
    );
    MessageType tableSchema = new MessageType("TestSchema",
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    PrimitiveType timestampField = result.getType("timestamp").asPrimitiveType();
    assertEquals(Type.Repetition.OPTIONAL, timestampField.getRepetition(),
        "Should preserve requested field's repetition");
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        timestampField.getLogicalTypeAnnotation());
  }

  @Test
  public void testRepairNestedGroupPreservesLogicalType() {
    // Test that repair preserves the group's logical type annotation
    GroupType nestedRequestedSchema = new GroupType(Type.Repetition.REQUIRED, "nested",
        LogicalTypeAnnotation.listType(),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .named("timestamp")
    );

    GroupType nestedTableSchema = new GroupType(Type.Repetition.REQUIRED, "nested",
        LogicalTypeAnnotation.listType(),
        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp")
    );

    MessageType requestedSchema = new MessageType("OuterRecord", nestedRequestedSchema);
    MessageType tableSchema = new MessageType("OuterRecord", nestedTableSchema);

    MessageType result = SchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    assertNotSame(requestedSchema, result);
    GroupType nestedResult = result.getType("nested").asGroupType();
    assertEquals(LogicalTypeAnnotation.listType(), nestedResult.getLogicalTypeAnnotation(),
        "Should preserve group's logical type annotation");
    PrimitiveType timestampField = nestedResult.getType("timestamp").asPrimitiveType();
    assertEquals(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS),
        timestampField.getLogicalTypeAnnotation());
  }
}
