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

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.HoodieAvroParquetSchemaConverter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests equivalence between {@link SchemaRepair} and {@link HoodieSchemaRepair}.
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
   * Helper method to verify that HoodieSchemaRepair and SchemaRepair produce equivalent results.
   */
  private void assertRepairEquivalence(HoodieSchema requestedSchema, HoodieSchema tableSchema) {
    // Apply Avro repair
    HoodieSchema repairedAvro = HoodieSchemaRepair.repairLogicalTypes(requestedSchema, tableSchema);

    // Convert to Parquet schemas
    MessageType requestedParquet = converter.convert(requestedSchema);
    MessageType tableParquet = converter.convert(tableSchema);

    // Apply Parquet repair
    MessageType repairedParquet = SchemaRepair.repairLogicalTypes(requestedParquet, tableParquet);

    // Convert repaired Parquet back to Avro
    HoodieSchema repairedParquetAsSchema = converter.convert(repairedParquet);

    // Verify equivalence
    assertEquals(repairedAvro, repairedParquetAsSchema,
        "SchemaRepair and HoodieSchemaRepair should produce equivalent results");
  }

  @Test
  public void testEquivalenceNoRepairNeeded() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));

    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceLongToLocalTimestampMillis() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));
    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceLongToLocalTimestampMicros() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));
    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMicros(), null, null)));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceTimestampMicrosToMillis() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));
    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMillis(), null, null)));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceNoRepairTimestampMillisToMicros() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));
    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createTimestampMicros(), null, null)));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceSimpleRecord() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));
    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordMultipleFields() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));
    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMicros(), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceNestedRecord() {
    HoodieSchema nestedRequestedSchema = HoodieSchema.createRecord("nestedrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)));

    HoodieSchema nestedTableSchema = HoodieSchema.createRecord("nestedrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)));

    HoodieSchema requestedSchema = HoodieSchema.createRecord("outerrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("nestedrecord", nestedRequestedSchema, null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("outerrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("nestedrecord", nestedTableSchema, null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordWithExtraFieldInRequested() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("newfield", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordMixedFields() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("newfield", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceNestedRecordWithExtraField() {
    HoodieSchema nestedRequestedSchema = HoodieSchema.createRecord("nestedrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("extrafield", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    HoodieSchema nestedTableSchema = HoodieSchema.createRecord("nestedrecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)));

    HoodieSchema requestedSchema = HoodieSchema.createRecord("outerrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("nestedrecord", nestedRequestedSchema, null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("outerrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("nestedrecord", nestedTableSchema, null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordFirstFieldChanged() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("timestamp1", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("timestamp2", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("timestamp1", HoodieSchema.createLocalTimestampMillis(), null, null),
        HoodieSchemaField.of("timestamp2", HoodieSchema.createLocalTimestampMicros(), null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordLastFieldChanged() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING), null, null),
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceComplexNestedStructure() {
    HoodieSchema innerRecordRequested = HoodieSchema.createRecord("inner", null, null, Arrays.asList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.INT), null, null)
    ));

    HoodieSchema innerRecordTable = HoodieSchema.createRecord("inner", null, null, Arrays.asList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null),
        HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.INT), null, null)
    ));

    HoodieSchema middleRecordRequested = HoodieSchema.createRecord("middle", null, null, Arrays.asList(
        HoodieSchemaField.of("inner", innerRecordRequested, null, null),
        HoodieSchemaField.of("middletimestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema middleRecordTable = HoodieSchema.createRecord("middle", null, null, Arrays.asList(
        HoodieSchemaField.of("inner", innerRecordTable, null, null),
        HoodieSchemaField.of("middletimestamp", HoodieSchema.createLocalTimestampMicros(), null, null)
    ));

    HoodieSchema requestedSchema = HoodieSchema.createRecord("outer", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("middle", middleRecordRequested, null, null),
        HoodieSchemaField.of("outertimestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("outer", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("middle", middleRecordTable, null, null),
        HoodieSchemaField.of("outertimestamp", HoodieSchema.createTimestampMicros(), null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceEmptyRecord() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("emptyrecord", null, null, Collections.emptyList());
    HoodieSchema tableSchema = HoodieSchema.createRecord("emptyrecord", null, null, Collections.emptyList());

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceRecordNoFieldsMatch() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("field1", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("field2", HoodieSchema.create(HoodieSchemaType.STRING), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("field3", HoodieSchema.create(HoodieSchemaType.INT), null, null),
        HoodieSchemaField.of("field4", HoodieSchema.createLocalTimestampMillis(), null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceMultipleTimestampRepairs() {
    HoodieSchema requestedSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("ts1", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("ts2", HoodieSchema.create(HoodieSchemaType.LONG), null, null),
        HoodieSchemaField.of("ts3", HoodieSchema.createTimestampMicros(), null, null),
        HoodieSchemaField.of("ts4", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("testrecord", null, null, Arrays.asList(
        HoodieSchemaField.of("ts1", HoodieSchema.createLocalTimestampMillis(), null, null),
        HoodieSchemaField.of("ts2", HoodieSchema.createLocalTimestampMicros(), null, null),
        HoodieSchemaField.of("ts3", HoodieSchema.createTimestampMillis(), null, null),
        HoodieSchemaField.of("ts4", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }

  @Test
  public void testEquivalenceDeepNesting() {
    HoodieSchema level3Requested = HoodieSchema.createRecord("level3", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG), null, null)
    ));

    HoodieSchema level3Table = HoodieSchema.createRecord("level3", null, null, Collections.singletonList(
        HoodieSchemaField.of("timestamp", HoodieSchema.createLocalTimestampMillis(), null, null)
    ));

    HoodieSchema level2Requested = HoodieSchema.createRecord("level2", null, null, Collections.singletonList(
        HoodieSchemaField.of("level3", level3Requested, null, null)
    ));

    HoodieSchema level2Table = HoodieSchema.createRecord("level2", null, null, Collections.singletonList(
        HoodieSchemaField.of("level3", level3Table, null, null)
    ));

    HoodieSchema level1Requested = HoodieSchema.createRecord("level1", null, null, Collections.singletonList(
        HoodieSchemaField.of("level2", level2Requested, null, null)
    ));

    HoodieSchema level1Table = HoodieSchema.createRecord("level1", null, null, Collections.singletonList(
        HoodieSchemaField.of("level2", level2Table, null, null)
    ));

    HoodieSchema requestedSchema = HoodieSchema.createRecord("level0", null, null, Collections.singletonList(
        HoodieSchemaField.of("level1", level1Requested, null, null)
    ));

    HoodieSchema tableSchema = HoodieSchema.createRecord("level0", null, null, Collections.singletonList(
        HoodieSchemaField.of("level1", level1Table, null, null)
    ));

    assertRepairEquivalence(requestedSchema, tableSchema);
  }
}
