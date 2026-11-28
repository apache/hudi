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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.buffer.BatchMatcher;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.table.read.BatchFileGroupMerger.OUTPUT_BATCH_ROWS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Merges base batches with buffered log records through {@link BatchFileGroupMerger}.
 */
class TestBatchFileGroupMerger {

  private static final Schema OUTPUT_SCHEMA = SchemaBuilder.record("row").fields().requiredString("value").endRecord();
  private static final Schema BASE_READ_SCHEMA = SchemaBuilder.record("row").fields()
      .requiredString("value").requiredString(HoodieRecord.RECORD_KEY_METADATA_FIELD).endRecord();

  /**
   * A batch is a list of rows; only batches built from log rows are used here.
   */
  private static final BatchSupport<IndexedRecord, List<IndexedRecord>> BATCHES = new BatchSupport<IndexedRecord, List<IndexedRecord>>() {
    @Override
    public ClosableIterator<List<IndexedRecord>> readBaseFile(HoodieBaseFile baseFile, HoodieSchema readSchema, boolean applyFilters) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int numRows(List<IndexedRecord> batch) {
      return batch.size();
    }

    @Override
    public boolean isNullAt(List<IndexedRecord> batch, int column, int row) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(List<IndexedRecord> batch, int column, int row) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object getEngineKey(List<IndexedRecord> batch, int column, int row) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object toEngineKey(String recordKey) {
      throw new UnsupportedOperationException();
    }

    @Override
    public IndexedRecord getRow(List<IndexedRecord> batch, int row) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<IndexedRecord> select(List<IndexedRecord> batch, int numOutputColumns, int numReadColumns, int[] rowIds, int numRows) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<IndexedRecord> toBatch(List<IndexedRecord> rows) {
      return rows;
    }
  };

  /**
   * A log-only file slice yields its log records one output batch at a time, reading no more of them than the batch
   * it returns needs.
   */
  @Test
  void testLeftoverLogRecordsAreReadOneBatchAtATime() {
    int numRecords = 2 * OUTPUT_BATCH_ROWS + 100;
    AtomicInteger consumed = new AtomicInteger();
    BatchMatcher<IndexedRecord> matcher = new BatchMatcher<IndexedRecord>() {
      @Override
      public String columnName() {
        return HoodieRecord.RECORD_KEY_METADATA_FIELD;
      }

      @Override
      public <B> boolean untouched(BatchSupport<IndexedRecord, B> batchSupport, B batch, int column, int numRows) {
        throw new UnsupportedOperationException();
      }

      @Override
      public <B> BufferedRecord<IndexedRecord> take(BatchSupport<IndexedRecord, B> batchSupport, B batch, int column, int row) {
        throw new UnsupportedOperationException();
      }

      @Override
      public Iterator<BufferedRecord<IndexedRecord>> leftovers() {
        return new Iterator<BufferedRecord<IndexedRecord>>() {
          @Override
          public boolean hasNext() {
            return consumed.get() < numRecords;
          }

          @Override
          public BufferedRecord<IndexedRecord> next() {
            String key = "key" + consumed.getAndIncrement();
            GenericData.Record row = new GenericData.Record(OUTPUT_SCHEMA);
            row.put("value", key);
            return new BufferedRecord<>(key, 0, row, null, null);
          }
        };
      }
    };
    HoodieSchema outputSchema = HoodieSchema.fromAvroSchema(OUTPUT_SCHEMA);
    BatchFileGroupMerger<IndexedRecord, List<IndexedRecord>> merger = new BatchFileGroupMerger<>(BATCHES, new EmptyIterator<>(),
        matcher, null, RecordMergeMode.COMMIT_TIME_ORDERING, AvroRecordContext.getFieldAccessorInstance(),
        HoodieSchema.fromAvroSchema(BASE_READ_SCHEMA), outputSchema, outputSchema, Collections.emptyList());

    List<String> values = new ArrayList<>();
    List<Integer> consumedBeforeBatch = new ArrayList<>();
    while (merger.hasNext()) {
      consumedBeforeBatch.add(consumed.get());
      merger.next().forEach(row -> values.add(row.get(0).toString()));
    }
    merger.close();
    assertEquals(Arrays.asList(OUTPUT_BATCH_ROWS, 2 * OUTPUT_BATCH_ROWS, numRecords), consumedBeforeBatch);
    assertEquals(numRecords, values.size());
    for (int i = 0; i < numRecords; i++) {
      assertEquals("key" + i, values.get(i));
    }
    assertFalse(merger.hasNext());
  }
}
