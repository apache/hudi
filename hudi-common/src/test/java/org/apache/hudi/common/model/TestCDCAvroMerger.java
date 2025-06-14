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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.CDCAvroMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestCDCAvroMerger {
  private CDCAvroTestMerger merger;
  private TypedProperties props;
  private Schema schema;

  @BeforeEach
  void setup() {
    merger = new CDCAvroTestMerger();
    props = new TypedProperties();
    schema = Schema.createRecord("Test", null, null, false);
    schema.setFields(Collections.singletonList(
        new Schema.Field("op", Schema.create(Schema.Type.STRING), null, (Object) null)));
  }

  @Test
  void testMerge_pickOldRecord() throws IOException {
    HoodieRecord oldRecord = mock(HoodieRecord.class);
    HoodieRecord newRecord = mock(HoodieRecord.class);

    HoodieAvroIndexedRecord oldIndexedRecord = mock(HoodieAvroIndexedRecord.class);
    HoodieAvroIndexedRecord newIndexedRecord = mock(HoodieAvroIndexedRecord.class);

    GenericRecord oldGeneric = new GenericData.Record(schema);
    oldGeneric.put("op", "I");
    GenericRecord newGeneric = new GenericData.Record(schema);
    newGeneric.put("op", "U");

    // Mocks for old record
    when(oldRecord.toIndexedRecord(schema, props))
        .thenReturn(Option.of(oldIndexedRecord));
    when(oldIndexedRecord.getData()).thenReturn(oldGeneric);

    // Mocks for new record
    when(newRecord.toIndexedRecord(schema, props))
        .thenReturn(Option.of(newIndexedRecord));
    when(newIndexedRecord.getData()).thenReturn(newGeneric);

    merger.setPickOld(true);
    merger.setDelete(false);

    Option<Pair<HoodieRecord, Schema>> result = merger.merge(oldRecord, schema, newRecord, schema, props);
    assertTrue(result.isPresent());
    assertSame(oldRecord, result.get().getLeft());
  }

  @Test
  void testMerge_deleteRecord() throws IOException {
    HoodieRecord oldRecord = mock(HoodieRecord.class);
    HoodieRecord newRecord = mock(HoodieRecord.class);

    HoodieAvroIndexedRecord oldIndexedRecord = mock(HoodieAvroIndexedRecord.class);
    HoodieAvroIndexedRecord newIndexedRecord = mock(HoodieAvroIndexedRecord.class);

    GenericRecord oldGeneric = new GenericData.Record(schema);
    oldGeneric.put("op", "I");
    GenericRecord newGeneric = new GenericData.Record(schema);
    newGeneric.put("op", "U");

    // Mocks for old record
    when(oldRecord.toIndexedRecord(schema, props))
        .thenReturn(Option.of(oldIndexedRecord));
    when(oldIndexedRecord.getData()).thenReturn(oldGeneric);

    // Mocks for new record
    when(newRecord.toIndexedRecord(schema, props))
        .thenReturn(Option.of(newIndexedRecord));
    when(newIndexedRecord.getData()).thenReturn(newGeneric);

    merger.setPickOld(false);
    merger.setDelete(true);

    Option<Pair<HoodieRecord, Schema>> result = merger.merge(oldRecord, schema, newRecord, schema, props);
    assertFalse(result.isPresent());
  }

  @Test
  void testMerge_enrichRecord() throws IOException {
    HoodieRecord oldRecord = mock(HoodieRecord.class);
    HoodieRecord newRecord = mock(HoodieRecord.class);

    HoodieAvroIndexedRecord oldIndexedRecord = mock(HoodieAvroIndexedRecord.class);
    HoodieAvroIndexedRecord newIndexedRecord = mock(HoodieAvroIndexedRecord.class);

    GenericRecord oldGeneric = new GenericData.Record(schema);
    oldGeneric.put("op", "I");
    GenericRecord newGeneric = new GenericData.Record(schema);
    newGeneric.put("op", "U");

    // Mocks for old record
    when(oldRecord.toIndexedRecord(schema, props))
        .thenReturn(Option.of(oldIndexedRecord));
    when(oldIndexedRecord.getData()).thenReturn(oldGeneric);

    // Mocks for new record
    when(newRecord.toIndexedRecord(schema, props))
        .thenReturn(Option.of(newIndexedRecord));
    when(newIndexedRecord.getData()).thenReturn(newGeneric);

    merger.setPickOld(false);
    merger.setDelete(false);

    Option<Pair<HoodieRecord, Schema>> result = merger.merge(oldRecord, schema, newRecord, schema, props);
    assertTrue(result.isPresent());
    assertEquals(HoodieRecord.HoodieRecordType.AVRO, merger.getRecordType());
  }

  static class CDCAvroTestMerger extends CDCAvroMerger {
    private boolean pickOld;
    private boolean delete;

    void setPickOld(boolean pickOld) {
      this.pickOld = pickOld;
    }

    void setDelete(boolean delete) {
      this.delete = delete;
    }

    @Override
    protected boolean shouldPickOldRecord(IndexedRecord oldRecord,
                                          IndexedRecord newRecord,
                                          Schema oldSchema,
                                          Schema newSchema) {
      return pickOld;
    }

    @Override
    protected boolean isDeleteRecord(GenericRecord record) {
      return delete;
    }

    @Override
    protected void postMergeOperation(IndexedRecord mergedRecord, IndexedRecord oldRecord) {
      // For testing enrichment logic if needed
    }
  }
}
