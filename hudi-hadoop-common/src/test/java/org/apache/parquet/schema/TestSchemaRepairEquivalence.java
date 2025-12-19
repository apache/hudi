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

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.HoodieAvroParquetSchemaConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests equivalence between {@link SchemaRepair} and {@link AvroSchemaRepair}.
 *
 * This test class verifies that both repair implementations produce logically
 * equivalent results when converting between Avro and Parquet schemas.
 */
public class TestSchemaRepairEquivalence {

  private HoodieAvroParquetSchemaConverter converter;

  @BeforeEach
  public void setUp() {
    converter = HoodieAvroParquetSchemaConverter.getAvroSchemaConverter(new Configuration());
  }

  /**
   * Helper method to verify that AvroSchemaRepair and SchemaRepair produce equivalent results.
   */
  private void assertRepairEquivalence(Schema requestedAvro, Schema tableAvro) {
    // Apply Avro repair
    Schema repairedAvro = AvroSchemaRepair.repairLogicalTypes(requestedAvro, tableAvro);

    // Convert to Parquet schemas
    MessageType requestedParquet = converter.convert(HoodieSchema.fromAvroSchema(requestedAvro));
    MessageType tableParquet = converter.convert(HoodieSchema.fromAvroSchema(tableAvro));

    // Apply Parquet repair
    MessageType repairedParquet = SchemaRepair.repairLogicalTypes(requestedParquet, tableParquet);

    // Convert repaired Parquet back to Avro
    HoodieSchema repairedParquetAsSchema = converter.convert(repairedParquet);

    // Verify equivalence
    assertEquals(repairedAvro, repairedParquetAsSchema.toAvroSchema(),
        "SchemaRepair and AvroSchemaRepair should produce equivalent results");
  }

  @Test
  public void testEquivalenceNoRepairNeeded() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("value").type().longType().noDefault()
        .endRecord();
    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("value").type().longType().noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceLongToLocalTimestampMillis() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .endRecord();
    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceLongToLocalTimestampMicros() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .endRecord();
    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceTimestampMicrosToMillis() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();
    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceNoRepairTimestampMillisToMicros() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();
    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceSimpleRecord() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordMultipleFields() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type().longType().noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceNestedRecord() {
    Schema nestedRequestedSchema = SchemaBuilder.record("nestedrecord")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .endRecord();

    Schema nestedTableSchema = SchemaBuilder.record("nestedrecord")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    Schema requestedSchema = SchemaBuilder.record("outerrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("nestedrecord").type(nestedRequestedSchema).noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("outerrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("nestedrecord").type(nestedTableSchema).noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordWithExtraFieldInRequested() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type().longType().noDefault()
        .name("newfield").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordMixedFields() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp").type().longType().noDefault()
        .name("newfield").type().stringType().noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .name("name").type().stringType().noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceNestedRecordWithExtraField() {
    Schema nestedRequestedSchema = SchemaBuilder.record("nestedrecord")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .name("extrafield").type().stringType().noDefault()
        .endRecord();

    Schema nestedTableSchema = SchemaBuilder.record("nestedrecord")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    Schema requestedSchema = SchemaBuilder.record("outerrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("nestedrecord").type(nestedRequestedSchema).noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("outerrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("nestedrecord").type(nestedTableSchema).noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordFirstFieldChanged() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp1").type().longType().noDefault()
        .name("timestamp2").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("timestamp1")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .name("timestamp2")
        .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordLastFieldChanged() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("name").type().stringType().noDefault()
        .name("timestamp").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("id").type().intType().noDefault()
        .name("name").type().stringType().noDefault()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceComplexNestedStructure() {
    Schema innerRecordRequested = SchemaBuilder.record("inner")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .name("value").type().intType().noDefault()
        .endRecord();

    Schema innerRecordTable = SchemaBuilder.record("inner")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .name("value").type().intType().noDefault()
        .endRecord();

    Schema middleRecordRequested = SchemaBuilder.record("middle")
        .fields()
        .name("inner").type(innerRecordRequested).noDefault()
        .name("middletimestamp").type().longType().noDefault()
        .endRecord();

    Schema middleRecordTable = SchemaBuilder.record("middle")
        .fields()
        .name("inner").type(innerRecordTable).noDefault()
        .name("middletimestamp")
        .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    Schema requestedSchema = SchemaBuilder.record("outer")
        .fields()
        .name("id").type().intType().noDefault()
        .name("middle").type(middleRecordRequested).noDefault()
        .name("outertimestamp").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("outer")
        .fields()
        .name("id").type().intType().noDefault()
        .name("middle").type(middleRecordTable).noDefault()
        .name("outertimestamp")
        .type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceEmptyRecord() {
    Schema requestedSchema = SchemaBuilder.record("emptyrecord").fields().endRecord();
    Schema tableSchema = SchemaBuilder.record("emptyrecord").fields().endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordNoFieldsMatch() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("field1").type().longType().noDefault()
        .name("field2").type().stringType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("field3").type().intType().noDefault()
        .name("field4")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceMultipleTimestampRepairs() {
    Schema requestedSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("ts1").type().longType().noDefault()
        .name("ts2").type().longType().noDefault()
        .name("ts3").type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
        .name("ts4").type().longType().noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("testrecord")
        .fields()
        .name("ts1")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .name("ts2")
        .type(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .name("ts3")
        .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .name("ts4").type().longType().noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceDeepNesting() {
    Schema level3Requested = SchemaBuilder.record("level3")
        .fields()
        .name("timestamp").type().longType().noDefault()
        .endRecord();

    Schema level3Table = SchemaBuilder.record("level3")
        .fields()
        .name("timestamp")
        .type(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
        .noDefault()
        .endRecord();

    Schema level2Requested = SchemaBuilder.record("level2")
        .fields()
        .name("level3").type(level3Requested).noDefault()
        .endRecord();

    Schema level2Table = SchemaBuilder.record("level2")
        .fields()
        .name("level3").type(level3Table).noDefault()
        .endRecord();

    Schema level1Requested = SchemaBuilder.record("level1")
        .fields()
        .name("level2").type(level2Requested).noDefault()
        .endRecord();

    Schema level1Table = SchemaBuilder.record("level1")
        .fields()
        .name("level2").type(level2Table).noDefault()
        .endRecord();

    Schema requestedSchema = SchemaBuilder.record("level0")
        .fields()
        .name("level1").type(level1Requested).noDefault()
        .endRecord();

    Schema tableSchema = SchemaBuilder.record("level0")
        .fields()
        .name("level1").type(level1Table).noDefault()
        .endRecord();

    assertRepairEquivalence(requestedSchema, tableSchema);
  }
}
