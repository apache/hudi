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

package org.apache.hudi.common.engine;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestHoodieReaderContext {
  private HoodieReaderContext readerContext;
  private IndexedRecord record;
  private Schema schema;

  @BeforeEach
  void setUp() {
    StorageConfiguration storageConfiguration = mock(StorageConfiguration.class);
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    readerContext = new DummyReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    record = mock(IndexedRecord.class);
    schema = mock(Schema.class);
  }

  @Test
  void testEventTimeFieldIsNull() {
    Option<Object> result = readerContext.getEventTime(record, schema, null);
    assertTrue(result.isEmpty());
  }

  @Test
  void testEventTimeFieldIsEmpty() {
    Option<Object> result = readerContext.getEventTime(record, schema, Option.empty());
    assertTrue(result.isEmpty());
  }

  @Test
  void testEventTimeFieldPresentButValueIsNull() {
    String fieldName = "event_time_return_null";
    Option<Object> result = readerContext.getEventTime(record, schema, Option.of(fieldName));
    assertTrue(result.isEmpty());
  }

  @Test
  void testEventTimeFieldPresentAndValueExists() {
    String fieldName = "event_time_return_123456789L";
    Object value = 123456789L;
    Option<Object> result = readerContext.getEventTime(record, schema, Option.of(fieldName));
    assertTrue(result.isPresent());
    assertEquals(value, result.get());
  }

  private static class DummyReaderContext extends HoodieReaderContext<IndexedRecord> {
    public DummyReaderContext(StorageConfiguration<?> storageConfiguration,
                              HoodieTableConfig tableConfig,
                              Option<InstantRange> instantRangeOpt,
                              Option<Predicate> keyFilterOpt) {
      super(storageConfiguration, tableConfig, instantRangeOpt, keyFilterOpt);
    }

    @Override
    public ClosableIterator<IndexedRecord> getFileRecordIterator(StoragePath filePath,
                                                                 long start,
                                                                 long length,
                                                                 Schema dataSchema,
                                                                 Schema requiredSchema,
                                                                 HoodieStorage storage) throws IOException {
      return null;
    }

    @Override
    public IndexedRecord convertAvroRecord(IndexedRecord avroRecord) {
      return null;
    }

    @Override
    public GenericRecord convertToAvroRecord(IndexedRecord record, Schema schema) {
      return null;
    }

    @Nullable
    @Override
    public IndexedRecord getDeleteRow(IndexedRecord record, String recordKey) {
      return null;
    }

    @Override
    protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode,
                                                         String mergeStrategyId,
                                                         String mergeImplClasses) {
      return null;
    }

    @Override
    public Object getValue(IndexedRecord record, Schema schema, String fieldName) {
      if (fieldName.equals("event_time_return_null")) {
        return null;
      } else {
        return 123456789L;
      }
    }

    @Override
    public String getMetaFieldValue(IndexedRecord record, int pos) {
      return null;
    }

    @Override
    public HoodieRecord<IndexedRecord> constructHoodieRecord(BufferedRecord<IndexedRecord> bufferedRecord) {
      return null;
    }

    @Override
    public IndexedRecord constructEngineRecord(Schema schema,
                                               Map<Integer, Object> values,
                                               BufferedRecord<IndexedRecord> engineRecord) {
      return null;
    }

    @Override
    public IndexedRecord seal(IndexedRecord record) {
      return null;
    }

    @Override
    public IndexedRecord toBinaryRow(Schema avroSchema, IndexedRecord record) {
      return null;
    }

    @Override
    public ClosableIterator<IndexedRecord> mergeBootstrapReaders(ClosableIterator<IndexedRecord> skeletonFileIterator, Schema skeletonRequiredSchema, ClosableIterator<IndexedRecord> dataFileIterator,
                                                                 Schema dataRequiredSchema, List<Pair<String, Object>> requiredPartitionFieldAndValues) {
      return null;
    }

    @Override
    public UnaryOperator<IndexedRecord> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
      return null;
    }
  }
}
