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

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodieLsmFileGroupReader {

  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("lsm_record", null, null, Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG))));

  private HoodieTableMetaClient metaClient;
  private TypedProperties props;
  private StorageConfiguration<?> storageConfiguration;

  @BeforeEach
  void setUp() throws IOException {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, "id");
    tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "ts");
    tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name());
    tableConfig.setValue(HoodieTableConfig.BASE_FILE_FORMAT, HoodieFileFormat.PARQUET.name());
    tableConfig.setValue(HoodieTableConfig.LOG_FILE_FORMAT, HoodieFileFormat.PARQUET.name());
    tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
    storageConfiguration = mock(StorageConfiguration.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.newInstance(any(StoragePath.class), any(StorageConfiguration.class))).thenReturn(storage);
    metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/tmp/lsm-reader-test"));
    when(metaClient.getStorage()).thenReturn(storage);
    doReturn(storageConfiguration).when(metaClient).getStorageConf();
    props = new TypedProperties();
  }

  @Test
  void testAllIteratorModesMergeSortedRecordsAndHandleDeletes() throws IOException {
    List<HoodieRecord> records = Arrays.asList(
        record("a", "old", 1),
        record("a", "new", 2),
        record("b", "kept", 1),
        delete("c", 3));

    try (HoodieLsmFileGroupReader<IndexedRecord> reader = reader(records, false);
         ClosableIterator<IndexedRecord> iterator = reader.getClosableIterator()) {
      List<String> values = new ArrayList<>();
      iterator.forEachRemaining(record -> values.add(record.get(0) + ":" + record.get(1)));
      assertEquals(Arrays.asList("a:new", "b:kept"), values);
      assertFalse(iterator.hasNext());
    }

    try (HoodieLsmFileGroupReader<IndexedRecord> reader = reader(records, false);
         ClosableIterator<HoodieRecord<IndexedRecord>> iterator = reader.getClosableHoodieRecordIterator()) {
      assertEquals(Arrays.asList("a", "b"), drainKeys(iterator));
    }

    try (HoodieLsmFileGroupReader<IndexedRecord> reader = reader(records, false);
         ClosableIterator<String> iterator = reader.getClosableKeyIterator()) {
      assertEquals(Arrays.asList("a", "b"), drain(iterator));
    }

    try (HoodieLsmFileGroupReader<IndexedRecord> reader = reader(records, true);
         ClosableIterator<BufferedRecord<IndexedRecord>> iterator = reader.getClosableBufferedRecordIterator()) {
      assertEquals("a", iterator.next().getRecordKey());
      assertEquals("b", iterator.next().getRecordKey());
      assertThrows(UnsupportedOperationException.class, iterator::hasNext);
    }
  }

  @Test
  void testLogOnlyIteratorAndRepeatedOpenClosePreviousIterator() throws IOException {
    HoodieLsmFileGroupReader<IndexedRecord> reader = reader(Arrays.asList(record("a", "one", 1), record("b", "two", 2)), false);
    ClosableIterator<BufferedRecord<IndexedRecord>> first = reader.getLogRecordsOnly();
    assertTrue(first.hasNext());

    try (ClosableIterator<String> replacement = reader.getClosableKeyIterator()) {
      assertEquals(Collections.emptyList(), drain(replacement));
    }
    first.close();
    reader.close();
  }

  @Test
  void testBuilderDefaultsAndRequiredArguments() {
    HoodieReaderContext<IndexedRecord> context = context();
    assertThrows(IllegalArgumentException.class, () -> HoodieLsmFileGroupReader.<IndexedRecord>builder()
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime("001")
        .withDataSchema(SCHEMA)
        .withRequestedSchema(SCHEMA)
        .withProps(props)
        .withPartitionPath("")
        .build());

    assertThrows(IllegalArgumentException.class, () -> HoodieLsmFileGroupReader.<IndexedRecord>builder()
        .withReaderContext(context)
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime("001")
        .withDataSchema(SCHEMA)
        .withRequestedSchema(SCHEMA)
        .withProps(props)
        .withPartitionPath("")
        .withStart(1L)
        .withLogFiles(Collections.singletonList(mock(HoodieLogFile.class)).stream())
        .build());
  }

  private HoodieLsmFileGroupReader<IndexedRecord> reader(List<HoodieRecord> records, boolean emitDeletes) {
    return HoodieLsmFileGroupReader.<IndexedRecord>builder()
        .withReaderContext(context())
        .withHoodieTableMetaClient(metaClient)
        .withLatestCommitTime("001")
        .withDataSchema(SCHEMA)
        .withRequestedSchema(SCHEMA)
        .withProps(props)
        .withRecordIterator(records.iterator())
        .withPartitionPath("")
        .withEmitDelete(emitDeletes)
        .build();
  }

  private HoodieReaderContext<IndexedRecord> context() {
    return new HoodieAvroReaderContext(storageConfiguration, metaClient.getTableConfig(), Option.empty(), Option.empty());
  }

  private static HoodieRecord record(String key, String value, long ts) {
    GenericData.Record record = new GenericData.Record(SCHEMA.toAvroSchema());
    record.put("id", key);
    record.put("value", value);
    record.put("ts", ts);
    return new HoodieAvroIndexedRecord(new HoodieKey(key, ""), record, ts);
  }

  private static HoodieRecord delete(String key, long ts) {
    return new HoodieEmptyRecord<>(new HoodieKey(key, ""), HoodieOperation.DELETE, ts, HoodieRecord.HoodieRecordType.AVRO);
  }

  private static <T> List<T> drain(ClosableIterator<T> iterator) {
    List<T> values = new ArrayList<>();
    iterator.forEachRemaining(values::add);
    return values;
  }

  private static List<String> drainKeys(ClosableIterator<HoodieRecord<IndexedRecord>> iterator) {
    List<String> keys = new ArrayList<>();
    iterator.forEachRemaining(record -> keys.add(record.getRecordKey()));
    return keys;
  }
}
