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

package org.apache.hudi.model;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.hudi.model.TestUtil.SCHEMA;
import static org.apache.hudi.model.TestUtil.getFieldFromAvroRecord;
import static org.apache.hudi.model.TestUtil.getFieldFromIndexedRecord;
import static org.apache.hudi.model.TestUtil.generateData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieAvroRecordMerger {
  private static final HoodieAvroRecordMerger MERGER = HoodieAvroRecordMerger.INSTANCE;

  @Test
  public void testMergeWhenBothSidesAreValid() throws IOException {
    List<HoodieAvroRecord> olderRecords = generateData(10);
    List<HoodieAvroRecord> newerRecords = generateData(10);

    // Update case.
    for (int i = 0; i < olderRecords.size(); ++i) {
      Option<Pair<HoodieRecord, Schema>> r = MERGER.merge(
          olderRecords.get(i),
          SCHEMA,
          newerRecords.get(i),
          SCHEMA,
          new TypedProperties());
      assertEquals(r.get().getRight(), SCHEMA);
      assertEquals(
          getFieldFromIndexedRecord(r.get().getLeft(), 0),
          getFieldFromAvroRecord(newerRecords.get(i), SCHEMA, "valueId"));
    }

    // Delete case 1: EmptyHoodieRecordPayload.
    Option<Pair<HoodieRecord, Schema>> r = MERGER.merge(
        olderRecords.get(0),
        SCHEMA,
        new HoodieAvroRecord(olderRecords.get(0).getKey(), new EmptyHoodieRecordPayload()),
        SCHEMA,
        new TypedProperties());
    assertFalse(r.isPresent());
  }

  @Test
  public void testMergeWhenAtLeastOneSideIsInvalid() throws IOException {
    HoodieAvroRecord record = generateData(1).get(0);

    // Old record is a delete record.
    Option<Pair<HoodieRecord, Schema>> r = MERGER.merge(
        new HoodieAvroRecord(record.getKey(), new EmptyHoodieRecordPayload()),
        SCHEMA,
        record,
        SCHEMA,
        new TypedProperties());
    assertTrue(r.isPresent());
    assertEquals(
        getFieldFromAvroRecord(record, SCHEMA, "valueId"),
        getFieldFromIndexedRecord(r.get().getLeft(), 0));

    // No meaningful new record.
    r = MERGER.merge(
        record,
        SCHEMA,
        new HoodieAvroRecord(record.getKey(), new EmptyHoodieRecordPayload()),
        SCHEMA,
        new TypedProperties());
    assertFalse(r.isPresent());

    // No meaningful records are provided.
    r = MERGER.merge(
        new HoodieAvroRecord(new HoodieKey(), new EmptyHoodieRecordPayload()),
        SCHEMA,
        new HoodieAvroRecord(new HoodieKey(), new EmptyHoodieRecordPayload()),
        SCHEMA,
        new TypedProperties());
    assertFalse(r.isPresent());
  }
}
