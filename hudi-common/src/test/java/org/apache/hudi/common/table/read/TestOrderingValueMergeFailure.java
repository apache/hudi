/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Demonstrates the merge failure a record built without an ordering value produces, which is the
 * shape HoodieStreamerUtils#createHoodieRecords emits for INSERT, and HoodieCreateRecordUtils
 * emitted for prepped Spark SQL writes.
 */
class TestOrderingValueMergeFailure {

  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
          + "{\"name\":\"_row_key\",\"type\":\"string\"},"
          + "{\"name\":\"ts\",\"type\":\"long\"}]}");
  private static final HoodieSchema HOODIE_SCHEMA = HoodieSchema.fromAvroSchema(SCHEMA);
  private static final String[] ORDERING_FIELDS = new String[] {"ts"};

  private static GenericRecord avroRecord(long ts) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("_row_key", "key1");
    record.put("ts", ts);
    return record;
  }

  /**
   * The incoming record is built through the overload that carries no ordering value, the base
   * record comes from storage with its real ts. Merging the two compares the incoming record's
   * ordering value against the base record's.
   */
  @Test
  void testMergingARecordBuiltWithoutAnOrderingValue() throws Exception {
    AvroRecordContext recordContext = new AvroRecordContext();
    HoodieReaderContext<IndexedRecord> readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    TypedProperties props = new TypedProperties();

    BufferedRecordMerger<IndexedRecord> merger = BufferedRecordMergerFactory.create(
        readerContext, RecordMergeMode.EVENT_TIME_ORDERING, false, Option.empty(),
        Option.empty(), HOODIE_SCHEMA, props, Option.empty());

    // Base record read from storage: ordering value extracted from the record, a Long.
    BufferedRecord<IndexedRecord> olderRecord = BufferedRecords.fromEngineRecord(
        avroRecord(200L), HOODIE_SCHEMA, recordContext, ORDERING_FIELDS, "key1", false);

    // Incoming record built the way HoodieStreamerUtils:138 builds it: no ordering value.
    HoodieRecord<?> incoming = HoodieRecordUtils.createHoodieRecord(
        avroRecord(100L), new HoodieKey("key1", "p"),
        DefaultHoodieRecordPayload.class.getName(), Option.empty(), true, false);
    BufferedRecord<IndexedRecord> newerRecord = BufferedRecords.fromHoodieRecord(
        incoming, HOODIE_SCHEMA, recordContext, props, ORDERING_FIELDS, false);

    BufferedRecord<IndexedRecord> merged = merger.finalMerge(olderRecord, newerRecord);

    assertNotNull(merged);
    // Event time ordering: the stored ts=200 must win over the incoming ts=100.
    assertEquals(200L, merged.getOrderingValue(),
        "the record with the higher event time must win");
  }
}
