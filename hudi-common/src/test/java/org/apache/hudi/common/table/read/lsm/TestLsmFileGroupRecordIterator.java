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
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemas;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestLsmFileGroupRecordIterator {

  @TempDir
  Path tempDir;

  @Test
  void testUpdateProcessorOnlyRunsForRecordsFromLogs() throws Exception {
    HoodieSchema schema = tableSchema();
    StoragePath baseFilePath = new StoragePath("/tmp/file1_1-0-1_001.parquet");
    HoodieBaseFile baseFile = mock(HoodieBaseFile.class);
    when(baseFile.getBootstrapBaseFile()).thenReturn(Option.empty());
    when(baseFile.getStoragePath()).thenReturn(baseFilePath);
    when(baseFile.getFileSize()).thenReturn(10L);
    HoodieLogFile logFile = logFile("file1_1-0-1_002_1.log.parquet", 10);

    InputSplit inputSplit = InputSplit.builder()
        .baseFileOption(Option.of(baseFile))
        .logFileStream(Stream.of(logFile))
        .partitionPath("")
        .start(0)
        .length(10)
        .build();
    HoodieReaderContext<String> readerContext = mock(HoodieReaderContext.class);
    FileGroupReaderSchemaHandler<String> schemaHandler = mock(FileGroupReaderSchemaHandler.class);
    RecordContext<String> recordContext = mock(RecordContext.class);
    DeleteContext deleteContext = mock(DeleteContext.class);
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    TypedProperties props = new TypedProperties();
    List<String> orderingFields = Collections.emptyList();

    when(readerContext.getSchemaHandler()).thenReturn(schemaHandler);
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(readerContext.getInstantRange()).thenReturn(Option.empty());
    when(readerContext.getMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
    when(readerContext.getRecordMerger()).thenReturn(Option.empty());
    when(readerContext.getPayloadClasses(props)).thenReturn(Option.empty());
    when(readerContext.getFileRecordIterator(baseFilePath, 0, 10, schema, schema, storage))
        .thenReturn(ClosableIterator.wrap(Arrays.asList("key1-base", "key3-base").iterator()));
    when(readerContext.getFileRecordIterator(logFile.getPath(), 0, 10, schema, schema, storage))
        .thenReturn(ClosableIterator.wrap(Arrays.asList("key2-log", "key3-log").iterator()));
    when(schemaHandler.getRequiredSchema()).thenReturn(schema);
    when(schemaHandler.getTableSchema()).thenReturn(schema);
    when(schemaHandler.getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(schemaHandler.getDeleteContext()).thenReturn(deleteContext);
    when(deleteContext.withReaderSchema(schema)).thenReturn(deleteContext);
    when(recordContext.seal(eq(schema), anyString())).thenAnswer(invocation -> invocation.getArgument(1));
    when(recordContext.getRecordKey(anyString(), eq(schema)))
        .thenAnswer(invocation -> ((String) invocation.getArgument(0)).substring(0, 4));
    when(recordContext.getOrderingValue(anyString(), eq(schema), eq(orderingFields))).thenReturn(1);
    when(recordContext.encodeSchema(schema)).thenReturn(1);
    when(recordContext.isDeleteRecord(anyString(), eq(deleteContext))).thenReturn(false);
    when(recordContext.getSchemaFromBufferRecord(any())).thenReturn(schema);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getPartialUpdateMode()).thenReturn(Option.empty());

    HoodieReadStats readStats = new HoodieReadStats();
    LsmFileGroupRecordIterator<String> iterator = new LsmFileGroupRecordIterator<>(
        readerContext, storage, inputSplit, orderingFields, metaClient, props,
        ReaderParameters.builder().build(), readStats, Option.empty());

    List<BufferedRecord<String>> records = new ArrayList<>();
    while (iterator.hasNext()) {
      records.add(iterator.next());
    }

    assertEquals(Arrays.asList("key1-base", "key2-log", "key3-log"),
        records.stream().map(BufferedRecord::getRecord).collect(Collectors.toList()));
    assertNull(records.get(0).getHoodieOperation());
    assertEquals(HoodieOperation.INSERT, records.get(1).getHoodieOperation());
    assertEquals(HoodieOperation.UPDATE_AFTER, records.get(2).getHoodieOperation());
    assertEquals(1, readStats.getNumInserts());
    assertEquals(1, readStats.getNumUpdates());
    verify(recordContext, times(1)).seal(schema, "key1-base");
    verify(recordContext, times(2)).getSchemaFromBufferRecord(any());

    iterator.close();
  }

  @Test
  void testDeleteLogSchemaUsesRecordKeyAndOrderingFields() {
    HoodieSchema deleteLogSchema = HoodieSchemas.createDeleteLogSchema(tableSchema(), Arrays.asList("ts"));

    assertEquals(Arrays.asList(HoodieRecord.RECORD_KEY_METADATA_FIELD, "ts"), deleteLogSchema.getFields().stream()
        .map(HoodieSchemaField::name)
        .collect(Collectors.toList()));
    assertEquals(HoodieSchemaType.STRING, deleteLogSchema.getField(HoodieRecord.RECORD_KEY_METADATA_FIELD).get().schema().getType());
    HoodieSchemaField orderingField = deleteLogSchema.getField("ts").get();
    assertTrue(orderingField.schema().isNullable());
    assertEquals(HoodieSchemaType.LONG, orderingField.getNonNullSchema().getType());
    assertEquals(HoodieSchema.NULL_VALUE, orderingField.defaultVal().get());
  }

  @Test
  void testLoserTreeMergesByRecordKeyThenMergeOrder() {
    List<LsmFileGroupRecordIterator.SortedRunReader<String>> readers = Arrays.asList(
        sortedRunReader(0, record("key1", "base-key1"), record("key3", "base-key3")),
        sortedRunReader(1, record("key1", "log1-key1"), record("key2", "log1-key2")),
        sortedRunReader(2, BufferedRecords.createDelete("key1", 3), record("key3", "log2-key3")));

    LsmFileGroupRecordIterator.LoserTree<String> loserTree = new LsmFileGroupRecordIterator.LoserTree<>(readers);

    assertEquals(Arrays.asList(
        "key1:base-key1",
        "key1:log1-key1",
        "key1:DELETE",
        "key2:log1-key2",
        "key3:base-key3",
        "key3:log2-key3"), drain(loserTree));
  }

  @Test
  void testSelectDirectLogReadersPrioritizesDeletesThenSmallFiles() {
    List<LsmFileGroupRecordIterator.LogReaderSpec> logReaderSpecs = Arrays.asList(
        new LsmFileGroupRecordIterator.LogReaderSpec(1, logFile("file1_1-0-1_001_1.log.parquet", 10)),
        new LsmFileGroupRecordIterator.LogReaderSpec(2, logFile("file1_1-0-1_002_1.deletes.parquet", 100)),
        new LsmFileGroupRecordIterator.LogReaderSpec(3, logFile("file1_1-0-1_003_1.log.parquet", 5)));

    Set<Integer> directReadersWithBase =
        LsmFileGroupRecordIterator.selectDirectLogMergeOrders(logReaderSpecs, true, 3);
    Set<Integer> directReadersWithoutBase =
        LsmFileGroupRecordIterator.selectDirectLogMergeOrders(logReaderSpecs, false, 2);

    assertEquals(new HashSet<>(Arrays.asList(2, 3)), directReadersWithBase);
    assertEquals(new HashSet<>(Arrays.asList(2, 3)), directReadersWithoutBase);
  }

  @Test
  void testSortedRunFixturesMergeAcrossBaseAndSpilledLogs() throws IOException {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, "id");
    tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "ts");
    tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, RecordMergeMode.EVENT_TIME_ORDERING.name());
    tableConfig.setValue(HoodieTableConfig.BASE_FILE_FORMAT, HoodieFileFormat.PARQUET.name());
    tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieAvroReaderContext context = org.mockito.Mockito.spy(
        new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty()));
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieReaderConfig.LSM_SORT_MERGE_SPILL_THRESHOLD.key(), "1");
    props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), tempDir.toString());
    context.initRecordMerger(props);

    FileGroupReaderSchemaHandler<IndexedRecord> schemaHandler = mock(FileGroupReaderSchemaHandler.class);
    when(schemaHandler.getRequiredSchema()).thenReturn(tableSchema());
    when(schemaHandler.getRequestedSchema()).thenReturn(tableSchema());
    when(schemaHandler.getTableSchema()).thenReturn(tableSchema());
    when(schemaHandler.getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(schemaHandler.getDeleteContext()).thenReturn(new DeleteContext(props, tableSchema()));
    when(schemaHandler.getRequiredSchemaForFileAndRenamedColumns(any(StoragePath.class)))
        .thenReturn(Pair.of(tableSchema(), Collections.emptyMap()));
    context.setSchemaHandler(schemaHandler);
    context.setTablePath("/tmp/lsm-fixture");

    doAnswer(invocation -> {
      StoragePathInfo pathInfo = invocation.getArgument(0);
      String name = pathInfo.getPath().getName();
      if (name.endsWith("deletes.parquet")) {
        HoodieSchema deleteSchema = invocation.getArgument(3);
        return ClosableIterator.wrap(Collections.singletonList(deleteRecord(deleteSchema, "b", 2L)).iterator());
      }
      List<IndexedRecord> records;
      if (name.endsWith(".parquet") && !name.contains(".log.")) {
        records = Arrays.asList(indexedRecord("a", "base-a", 1), indexedRecord("c", "base-c", 5));
      } else if (name.contains("_002_")) {
        records = Arrays.asList(indexedRecord("a", "log2-a", 3), indexedRecord("c", "stale-c", 2));
      } else {
        records = Arrays.asList(indexedRecord("a", "log1-a", 2), indexedRecord("b", "log1-b", 1));
      }
      return ClosableIterator.wrap(records.iterator());
    }).when(context).getFileRecordIterator(any(StoragePathInfo.class), anyLong(), anyLong(),
        any(HoodieSchema.class), any(HoodieSchema.class), any(HoodieStorage.class));

    HoodieBaseFile baseFile = new HoodieBaseFile(pathInfo("/tmp/file1_1-0-1_001.parquet", 100));
    List<HoodieLogFile> logs = Arrays.asList(
        new HoodieLogFile(pathInfo("/tmp/file1_1-0-1_001_1.log.parquet", 50)),
        new HoodieLogFile(pathInfo("/tmp/file1_1-0-1_002_1.log.parquet", 60)),
        new HoodieLogFile(pathInfo("/tmp/file1_1-0-1_003_1.deletes.parquet", 10)));
    InputSplit split = InputSplit.builder()
        .baseFileOption(Option.of(baseFile))
        .logFileStream(logs.stream())
        .partitionPath("")
        .start(0)
        .length(Long.MAX_VALUE)
        .build();
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    ReaderParameters parameters = ReaderParameters.builder().emitDeletes(false).build();

    List<String> actual = new ArrayList<>();
    try (LsmFileGroupRecordIterator<IndexedRecord> iterator = new LsmFileGroupRecordIterator<>(
        context, mock(HoodieStorage.class), split, Collections.singletonList("ts"), metaClient,
        props, parameters, new HoodieReadStats(), Option.empty())) {
      assertTrue(iterator.hasNext());
      assertTrue(iterator.hasNext());
      while (iterator.hasNext()) {
        BufferedRecord<IndexedRecord> record = iterator.next();
        actual.add(record.getRecordKey() + ":" + record.getRecord().get(1));
      }
      assertFalse(iterator.hasNext());
      assertThrows(java.util.NoSuchElementException.class, iterator::next);
    }

    assertEquals(Arrays.asList("a:log2-a", "c:base-c"), actual);

    List<String> logOnly = new ArrayList<>();
    try (LsmFileGroupRecordIterator<IndexedRecord> iterator = new LsmFileGroupRecordIterator<>(
        context, mock(HoodieStorage.class), split, Collections.singletonList("ts"), metaClient,
        props, parameters, new HoodieReadStats(), Option.empty(), false)) {
      while (iterator.hasNext()) {
        BufferedRecord<IndexedRecord> record = iterator.next();
        logOnly.add(record.getRecordKey() + ":" + record.getRecord().get(1));
      }
    }
    assertEquals(Arrays.asList("a:log2-a", "c:stale-c"), logOnly);
  }

  private static LsmFileGroupRecordIterator.SortedRunReader<String> sortedRunReader(int mergeOrder,
                                                                                    BufferedRecord<String>... records) {
    LsmFileGroupRecordIterator.SortedRunReader<String> reader = new LsmFileGroupRecordIterator.SortedRunReader<>(
        mergeOrder, ClosableIterator.wrap(Arrays.asList(records).iterator()));
    assertTrue(reader.advance());
    return reader;
  }

  private static BufferedRecord<String> record(String recordKey, String value) {
    return new BufferedRecord<>(recordKey, 1, value, null, null);
  }

  private static HoodieSchema tableSchema() {
    return HoodieSchema.createRecord("test_record", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("value", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG))));
  }

  private static IndexedRecord indexedRecord(String key, String value, long ts) {
    GenericData.Record record = new GenericData.Record(tableSchema().toAvroSchema());
    record.put("id", key);
    record.put("value", value);
    record.put("ts", ts);
    return record;
  }

  private static IndexedRecord deleteRecord(HoodieSchema schema, String key, long ts) {
    GenericData.Record record = new GenericData.Record(schema.toAvroSchema());
    record.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, key);
    record.put("ts", ts);
    return record;
  }

  private static StoragePathInfo pathInfo(String path, long size) {
    return new StoragePathInfo(new StoragePath(path), size, false, (short) 1, 1024, 0);
  }

  private static List<String> drain(LsmFileGroupRecordIterator.LoserTree<String> loserTree) {
    List<String> records = new ArrayList<>();
    while (!loserTree.isEmpty()) {
      BufferedRecord<String> record = loserTree.popWinner();
      records.add(record.getRecordKey() + ":" + (record.isDelete() ? "DELETE" : record.getRecord()));
    }
    return records;
  }

  private static HoodieLogFile logFile(String fileName, long fileSize) {
    return new HoodieLogFile(new StoragePath("/tmp/" + fileName), fileSize);
  }
}
