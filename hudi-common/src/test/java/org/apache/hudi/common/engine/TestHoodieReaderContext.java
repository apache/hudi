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
import org.apache.hudi.common.expression.Predicate;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.DefaultJavaTypeConverter;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieReaderContext {

  private static final HoodieSchema READER_SCHEMA = HoodieSchema.createRecord("TestRecord", null, null, new ArrayList<>());

  @Test
  void defaultLookupRecordsFiltersByFullKey() throws IOException {
    TestReaderContext readerContext = new TestReaderContext(Arrays.asList("id1", "id2", "id20"));

    List<String> actual = lookup(readerContext, Arrays.asList("id2", "missing"), true);

    assertEquals(Arrays.asList("id2"), actual);
  }

  @Test
  void defaultLookupRecordsFiltersByKeyPrefix() throws IOException {
    TestReaderContext readerContext = new TestReaderContext(Arrays.asList("id1", "id2", "id20", "other"));

    List<String> actual = lookup(readerContext, Arrays.asList("id2"), false);

    assertEquals(Arrays.asList("id2", "id20"), actual);
  }

  private static List<String> lookup(TestReaderContext readerContext, List<String> keys, boolean fullKey) throws IOException {
    StoragePath filePath = new StoragePath("/tmp/test.parquet");
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getPathInfo(filePath)).thenReturn(new StoragePathInfo(filePath, 100, false, (short) 1, 100, 0));

    List<String> result = new ArrayList<>();
    try (ClosableIterator<String> iterator = readerContext.lookupRecords(
        filePath, HoodieFileFormat.PARQUET, READER_SCHEMA, storage, keys, fullKey)) {
      iterator.forEachRemaining(result::add);
    }
    return result;
  }

  private static class TestReaderContext extends HoodieReaderContext<String> {
    private final List<String> records;

    private TestReaderContext(List<String> records) {
      super(mock(StorageConfiguration.class), tableConfig(), Option.<InstantRange>empty(), Option.<Predicate>empty(), new TestRecordContext());
      this.records = records;
    }

    @Override
    public ClosableIterator<String> getFileRecordIterator(
        StoragePath filePath,
        long start,
        long length,
        HoodieSchema dataSchema,
        HoodieSchema requiredSchema,
        HoodieStorage storage) {
      return ClosableIterator.wrap(records.iterator());
    }

    @Override
    protected Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
      return Option.empty();
    }

    @Override
    public ClosableIterator<String> mergeBootstrapReaders(
        ClosableIterator<String> skeletonFileIterator,
        HoodieSchema skeletonRequiredSchema,
        ClosableIterator<String> dataFileIterator,
        HoodieSchema dataRequiredSchema,
        List<Pair<String, Object>> requiredPartitionFieldAndValues) {
      return ClosableIterator.wrap(new ArrayList<String>().iterator());
    }

    private static HoodieTableConfig tableConfig() {
      HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
      when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
      return tableConfig;
    }
  }

  private static class TestRecordContext extends RecordContext<String> {
    private TestRecordContext() {
      super(new DefaultJavaTypeConverter());
    }

    @Override
    public String getRecordKey(String record, HoodieSchema schema) {
      return record;
    }

    @Override
    public HoodieRecord<String> constructHoodieRecord(BufferedRecord<String> bufferedRecord, String partitionPath) {
      return null;
    }

    @Override
    public String mergeWithEngineRecord(HoodieSchema schema, Map<Integer, Object> updateValues, BufferedRecord<String> baseRecord) {
      return null;
    }

    @Override
    public String constructEngineRecord(HoodieSchema recordSchema, Object[] fieldValues) {
      return null;
    }

    @Override
    public HoodieRecord.HoodieRecordType getEngineRecordType() {
      return HoodieRecord.HoodieRecordType.AVRO;
    }

    @Override
    public Object getValue(String record, HoodieSchema schema, String fieldName) {
      return null;
    }

    @Override
    public String getMetaFieldValue(String record, int pos) {
      return null;
    }

    @Override
    public String convertAvroRecord(IndexedRecord avroRecord) {
      return null;
    }

    @Override
    public GenericRecord convertToAvroRecord(String record, HoodieSchema schema) {
      return null;
    }

    @Override
    public String getDeleteRow(String recordKey) {
      return null;
    }

    @Override
    public String seal(HoodieSchema schema, String record) {
      return record;
    }

    @Override
    public String toBinaryRow(HoodieSchema schema, String record) {
      return record;
    }

    @Override
    public UnaryOperator<String> projectRecord(HoodieSchema from, HoodieSchema to, Map<String, String> renamedColumns) {
      return UnaryOperator.identity();
    }
  }
}
