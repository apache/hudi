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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.read.BaseFileUpdateCallback;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.TableState;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.SKIP_DELETE_BLOCK_CONTENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestPositionBasedFileGroupRecordBuffer {

  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("record", null, null, Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
      HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG))));

  @TempDir
  Path tempDir;

  @Test
  void testPositionHeaderValidationAndKeyFiltering() throws Exception {
    HoodieLogBlock block = mock(HoodieLogBlock.class);
    when(block.getBaseFileInstantTimeOfPositions()).thenReturn(null, "002", "001", "001");
    when(block.getRecordPositionList()).thenReturn(Collections.emptyList(), Arrays.asList(2L, 4L));

    assertNull(PositionBasedFileGroupRecordBuffer.extractRecordPositions(block, "001"));
    assertNull(PositionBasedFileGroupRecordBuffer.extractRecordPositions(block, "001"));
    assertNull(PositionBasedFileGroupRecordBuffer.extractRecordPositions(block, "001"));
    assertEquals(Arrays.asList(2L, 4L), PositionBasedFileGroupRecordBuffer.extractRecordPositions(block, "001"));

    PositionBasedFileGroupRecordBuffer<IndexedRecord> buffer = createBuffer();
    IndexedRecord a = record("alpha", 1);
    assertFalse(buffer.shouldSkip(a, true, Collections.emptySet(), SCHEMA));
    assertFalse(buffer.shouldSkip(a, true, Collections.singleton("alpha"), SCHEMA));
    assertTrue(buffer.shouldSkip(a, true, Collections.singleton("beta"), SCHEMA));
    assertFalse(buffer.shouldSkip(a, false, Collections.singleton("al"), SCHEMA));
    assertTrue(buffer.shouldSkip(a, false, Collections.singleton("be"), SCHEMA));
    assertThrows(HoodieKeyException.class, () -> buffer.shouldSkip(record("", 1), true,
        new HashSet<>(Collections.singletonList("alpha")), SCHEMA));
    assertEquals(HoodieFileGroupRecordBuffer.BufferType.POSITION_BASED_MERGE, buffer.getBufferType());
    buffer.close();
  }

  @Test
  void testBlockCardinalityGuardsAndContainsLogRecord() throws Exception {
    PositionBasedFileGroupRecordBuffer<IndexedRecord> buffer = createBuffer();
    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getBaseFileInstantTimeOfPositions()).thenReturn("001");
    when(dataBlock.getRecordPositionList()).thenReturn(Collections.singletonList(0L));
    when(dataBlock.containsPartialUpdates()).thenReturn(false);
    when(dataBlock.getSchema()).thenReturn(SCHEMA);
    when(dataBlock.getEngineRecordIterator(buffer.readerContext)).thenReturn(ClosableIterator.wrap(Collections.emptyIterator()));
    assertThrows(HoodieException.class, () -> buffer.processDataBlock(dataBlock, Option.empty()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getBaseFileInstantTimeOfPositions()).thenReturn("001");
    when(deleteBlock.getRecordPositionList()).thenReturn(Collections.singletonList(0L));
    when(deleteBlock.getRecordsToDelete(buffer.readerContext)).thenReturn(Collections.emptyList());
    assertThrows(HoodieException.class, () -> buffer.processDeleteBlock(deleteBlock));

    IndexedRecord alpha = record("alpha", 1);
    buffer.processNextDataRecord(BufferedRecords.fromEngineRecord(
        alpha, SCHEMA, buffer.readerContext.getRecordContext(), Collections.emptyList(), false), 0L);
    assertTrue(buffer.containsLogRecord("alpha"));
    assertFalse(buffer.containsLogRecord("missing"));

    buffer.readerContext.setShouldMergeUseRecordPosition(false);
    HoodieDataBlock emptyBlock = mock(HoodieDataBlock.class);
    when(emptyBlock.getEngineRecordIterator(buffer.readerContext)).thenReturn(ClosableIterator.wrap(Collections.emptyIterator()));
    when(emptyBlock.getSchema()).thenReturn(SCHEMA);
    buffer.processDataBlock(emptyBlock, Option.empty());
    assertEquals(HoodieFileGroupRecordBuffer.BufferType.KEY_BASED_MERGE, buffer.getBufferType());
    buffer.close();
  }

  /**
   * Under commit-time ordering the merge with deletes taken from the delete block header equals the merge with
   * deletes read from the block content, including after the deletes spill to disk; under event-time ordering the
   * property is ignored and the content is read.
   */
  @ParameterizedTest
  @CsvSource({"COMMIT_TIME_ORDERING,false", "COMMIT_TIME_ORDERING,true", "EVENT_TIME_ORDERING,false"})
  void testSkipDeleteBlockContentMergesLikeReadingIt(RecordMergeMode mergeMode, boolean spill) throws Exception {
    // k1 updated, k2 deleted then written again, k4 deleted, all by position
    List<String> expected = Arrays.asList("k0:1", "k1:2", "k2:4", "k3:1");
    for (boolean skip : new boolean[] {false, true}) {
      PositionBasedFileGroupRecordBuffer<IndexedRecord> buffer = createBuffer(mergeMode, props(skip, spill), false, Option.empty());
      buffer.processDataBlock(dataBlock(Arrays.asList(1L, 2L), record("k1", 2), record("k2", 2)), Option.empty());
      HoodieDeleteBlock deleteBlock = deleteBlock(Arrays.asList(2L, 4L), "k2", "k4");
      buffer.processDeleteBlock(deleteBlock);
      buffer.processDataBlock(dataBlock(Collections.singletonList(2L), record("k2", 4)), Option.empty());
      assertEquals(expected, mergeWithBase(buffer, 5), "skip=" + skip);
      if (skip && mergeMode == RecordMergeMode.COMMIT_TIME_ORDERING) {
        verify(deleteBlock, never()).getRecordsToDelete(any(HoodieReaderContext.class));
      } else {
        verify(deleteBlock).getRecordsToDelete(any(HoodieReaderContext.class));
      }
    }
  }

  /**
   * A delete taken by position from the header yields to a record buffered by key from a later block that has no
   * record positions.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testSkippedDeletesYieldToLaterRecordsAfterFallbackToKeys(boolean skip) throws Exception {
    PositionBasedFileGroupRecordBuffer<IndexedRecord> buffer =
        createBuffer(RecordMergeMode.COMMIT_TIME_ORDERING, props(skip, false), false, Option.empty());
    buffer.processDeleteBlock(deleteBlock(Arrays.asList(0L, 1L), "k0", "k1"));
    buffer.processDataBlock(dataBlock(null, record("k1", 5), record("k2", 5), record("k3", 5)), Option.empty());
    buffer.processDeleteBlock(deleteBlock(null, "k0", "k2"));
    assertEquals(HoodieFileGroupRecordBuffer.BufferType.KEY_BASED_MERGE, buffer.getBufferType());
    assertEquals(skip ? new HashSet<>(Arrays.asList(0L, 1L, "k0", "k1", "k2", "k3")) : new HashSet<>(Arrays.asList("k0", "k1", "k2", "k3")),
        new HashSet<>(buffer.getLogRecords().keySet()));
    assertEquals(Arrays.asList("k1:5", "k3:5"), mergeWithBase(buffer, 4));
  }

  /**
   * Readers that emit deletes or report them to a callback need the keys of deleted records, so they read the
   * delete block content even when the property is set.
   */
  @Test
  void testSkipDeleteBlockContentIsIgnoredWhenDeleteKeysAreNeeded() throws Exception {
    HoodieDeleteBlock emittedDeletes = deleteBlock(Collections.singletonList(1L), "k1");
    createBuffer(RecordMergeMode.COMMIT_TIME_ORDERING, props(true, false), true, Option.empty()).processDeleteBlock(emittedDeletes);
    verify(emittedDeletes).getRecordsToDelete(any(HoodieReaderContext.class));

    BaseFileUpdateCallback<IndexedRecord> callback = mock(BaseFileUpdateCallback.class);
    PositionBasedFileGroupRecordBuffer<IndexedRecord> buffer =
        createBuffer(RecordMergeMode.COMMIT_TIME_ORDERING, props(true, false), false, Option.of(callback));
    buffer.processDeleteBlock(deleteBlock(Collections.singletonList(1L), "k1"));
    assertEquals(Arrays.asList("k0:1", "k2:1"), mergeWithBase(buffer, 3));
    verify(callback).onDelete(eq("k1"), any(), any());
    verify(callback, never()).onDelete(isNull(), any(), any());
  }

  /**
   * The file group reader returns the log records keyed by base file row position when every block carries
   * positions for the base file, and by record key once a block does not, without opening the base file. The
   * reader property to skip delete block content reaches the buffer.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFileGroupReaderReturnsLogRecordsWithoutOpeningBaseFile(boolean positioned) throws Exception {
    HoodieTableConfig tableConfig = tableConfig(RecordMergeMode.COMMIT_TIME_ORDERING);
    HoodieReaderContext<IndexedRecord> readerContext = spy(new HoodieAvroReaderContext(
        mock(StorageConfiguration.class), tableConfig, Option.empty(), Option.empty()));
    HoodieDeleteBlock deleteBlock = deleteBlock(Collections.singletonList(2L), "k2");
    FileGroupRecordBufferLoader<IndexedRecord> loader = (context, storage, split, orderingFields, tableState, props,
                                                         readerParameters, readStats, callback) -> {
      PositionBasedFileGroupRecordBuffer<IndexedRecord> buffer = new PositionBasedFileGroupRecordBuffer<>(
          context, context.getMergeMode(), Option.empty(), split.getBaseFileOption().get().getCommitTime(), props, orderingFields,
          UpdateProcessor.create(readStats, context, readerParameters.isEmitDeletes(), callback, props));
      try {
        buffer.processDataBlock(dataBlock(positioned ? Arrays.asList(1L, 3L) : null, record("k1", 2), record("k3", 2)), Option.empty());
        buffer.processDeleteBlock(deleteBlock);
      } catch (IOException e) {
        throw new HoodieException(e);
      }
      return Pair.of(buffer, Collections.singletonList("002"));
    };
    try (HoodieFileGroupReader<IndexedRecord> reader = fileGroupReader(readerContext, tableConfig, loader, false)) {
      Map<Serializable, BufferedRecord<IndexedRecord>> records = reader.getLogRecordsByPositionOrKey();
      assertEquals(positioned, readerContext.getShouldMergeUseRecordPosition());
      assertEquals(positioned ? new HashSet<>(Arrays.asList(1L, 2L, 3L)) : new HashSet<>(Arrays.asList("k1", "k2", "k3")),
          new HashSet<>(records.keySet()));
      assertEquals(Arrays.asList("k1:2", "k3:2"), records.values().stream().filter(record -> !record.isDelete())
          .map(record -> format(record.getRecord())).sorted().collect(Collectors.toList()));
      assertTrue(records.get(positioned ? 2L : "k2").isDelete());
      assertEquals(Collections.singletonList("002"), reader.getValidBlockInstants());
      assertThrows(IllegalStateException.class, reader::getLogRecordsByPositionOrKey);
    }
    if (positioned) {
      verify(deleteBlock, never()).getRecordsToDelete(any(HoodieReaderContext.class));
    } else {
      verify(deleteBlock).getRecordsToDelete(any(HoodieReaderContext.class));
    }
    verify(readerContext, never()).getFileRecordIterator(any(StoragePath.class), anyLong(), anyLong(), any(), any(), any());
    verify(readerContext, never()).getFileRecordIterator(any(StoragePathInfo.class), anyLong(), anyLong(), any(), any(), any());
  }

  /**
   * Readers whose buffers are not keyed by position or record key do not return log records.
   */
  @Test
  void testFileGroupReaderRejectsLogRecordsForUnsupportedBuffers() throws Exception {
    HoodieTableConfig tableConfig = tableConfig(RecordMergeMode.COMMIT_TIME_ORDERING);
    HoodieFileGroupRecordBuffer<IndexedRecord> unmerged = mock(HoodieFileGroupRecordBuffer.class);
    when(unmerged.getBufferType()).thenReturn(HoodieFileGroupRecordBuffer.BufferType.UNMERGED);
    FileGroupRecordBufferLoader<IndexedRecord> loader = (context, storage, split, orderingFields, tableState, props,
                                                         readerParameters, readStats, callback) -> Pair.of(unmerged, Collections.emptyList());
    try (HoodieFileGroupReader<IndexedRecord> reader = fileGroupReader(readerContext(tableConfig), tableConfig, loader, true)) {
      assertThrows(IllegalArgumentException.class, reader::getLogRecordsByPositionOrKey);
    }
    try (HoodieFileGroupReader<IndexedRecord> reader = fileGroupReader(readerContext(tableConfig), tableConfig, loader, false)) {
      assertThrows(IllegalStateException.class, reader::getLogRecordsByPositionOrKey);
    }
  }

  /**
   * The reusable loader keeps the log records in a map shared by its readers, so a reader built with it does not
   * return log records, and is rejected before it reads a log file.
   */
  @Test
  void testFileGroupReaderRejectsLogRecordsForReusableLoader() throws Exception {
    HoodieTableConfig tableConfig = tableConfig(RecordMergeMode.COMMIT_TIME_ORDERING);
    FileGroupRecordBufferLoader<IndexedRecord> loader = spy(FileGroupRecordBufferLoader.createReusable(readerContext(tableConfig)));
    try (HoodieFileGroupReader<IndexedRecord> reader = fileGroupReader(readerContext(tableConfig), tableConfig, loader, false)) {
      IllegalArgumentException e = assertThrows(IllegalArgumentException.class, reader::getLogRecordsByPositionOrKey);
      assertEquals("Log records are not returned for a reader that reuses the log records of another reader", e.getMessage());
    }
    verify(loader, never()).getRecordBuffer(any(), any(), any(), any(), any(), any(), any(), any(), any());
  }

  private HoodieFileGroupReader<IndexedRecord> fileGroupReader(HoodieReaderContext<IndexedRecord> readerContext, HoodieTableConfig tableConfig,
                                                               FileGroupRecordBufferLoader<IndexedRecord> loader, boolean sortOutput) {
    String tablePath = tempDir.toUri().toString();
    return HoodieFileGroupReader.<IndexedRecord>builder()
        .withReaderContext(readerContext)
        .withTableState(TableState.of(new StoragePath(tablePath), tableConfig, Option.empty(), Option.empty()))
        .withStorage(mock(HoodieStorage.class))
        .withLatestCommitTime("002")
        .withDataSchema(SCHEMA)
        .withRequestedSchema(SCHEMA)
        .withProps(props(true, false))
        .withBaseFileOption(Option.of(new HoodieBaseFile(tablePath + "/f1_1-0-1_001.parquet")))
        .withLogFiles(Stream.of(new HoodieLogFile(new StoragePath(tablePath + "/.f1_001.log.1_1-0-1"))))
        .withPartitionPath("")
        .withShouldUseRecordPosition(true)
        .withSortOutput(sortOutput)
        .withRecordBufferLoader(loader)
        .build();
  }

  private PositionBasedFileGroupRecordBuffer<IndexedRecord> createBuffer() {
    HoodieTableConfig tableConfig = tableConfig(RecordMergeMode.COMMIT_TIME_ORDERING);
    HoodieReaderContext<IndexedRecord> context = readerContext(tableConfig);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    return new PositionBasedFileGroupRecordBuffer<>(context, metaClient, RecordMergeMode.COMMIT_TIME_ORDERING,
        Option.empty(), "001", props(false, false), Collections.emptyList(), mock(UpdateProcessor.class));
  }

  private PositionBasedFileGroupRecordBuffer<IndexedRecord> createBuffer(RecordMergeMode mergeMode, TypedProperties props, boolean emitDeletes,
                                                                         Option<BaseFileUpdateCallback<IndexedRecord>> callback) {
    HoodieReaderContext<IndexedRecord> context = readerContext(tableConfig(mergeMode));
    List<String> orderingFields = mergeMode == RecordMergeMode.EVENT_TIME_ORDERING ? Collections.singletonList("ts") : Collections.emptyList();
    return new PositionBasedFileGroupRecordBuffer<>(context, mergeMode, Option.empty(), "001", props, orderingFields,
        UpdateProcessor.create(new HoodieReadStats(), context, emitDeletes, callback, props));
  }

  private static HoodieTableConfig tableConfig(RecordMergeMode mergeMode) {
    HoodieTableConfig tableConfig = new HoodieTableConfig();
    tableConfig.setValue(HoodieTableConfig.RECORDKEY_FIELDS, "id");
    tableConfig.setValue(HoodieTableConfig.RECORD_MERGE_MODE, mergeMode.name());
    if (mergeMode == RecordMergeMode.EVENT_TIME_ORDERING) {
      tableConfig.setValue(HoodieTableConfig.ORDERING_FIELDS, "ts");
    }
    tableConfig.setValue(HoodieTableConfig.BASE_FILE_FORMAT, HoodieFileFormat.PARQUET.name());
    tableConfig.setValue(HoodieTableConfig.META_FIELDS_MODE, "NONE");
    return tableConfig;
  }

  private static HoodieReaderContext<IndexedRecord> readerContext(HoodieTableConfig tableConfig) {
    HoodieReaderContext<IndexedRecord> context = new HoodieAvroReaderContext(
        mock(StorageConfiguration.class), tableConfig, Option.empty(), Option.empty());
    context.initRecordMerger(new TypedProperties());
    context.setShouldMergeUseRecordPosition(true);
    FileGroupReaderSchemaHandler<IndexedRecord> schemaHandler = mock(FileGroupReaderSchemaHandler.class);
    when(schemaHandler.getTableSchema()).thenReturn(SCHEMA);
    when(schemaHandler.getRequiredSchema()).thenReturn(SCHEMA);
    when(schemaHandler.getSchemaForUpdates()).thenReturn(SCHEMA);
    when(schemaHandler.getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(schemaHandler.getSchemaEvolutionTransformer(any(), any())).thenReturn(Option.empty());
    when(schemaHandler.getDeleteContext()).thenReturn(new DeleteContext(new TypedProperties(), SCHEMA));
    context.setSchemaHandler(schemaHandler);
    return context;
  }

  private TypedProperties props(boolean skipDeleteBlockContent, boolean spill) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), tempDir.toString());
    if (skipDeleteBlockContent) {
      props.setProperty(SKIP_DELETE_BLOCK_CONTENT, "true");
    }
    if (spill) {
      props.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), "0");
    }
    return props;
  }

  /**
   * A data block with the given base file row positions, or without positions when {@code positions} is null.
   */
  private static HoodieDataBlock dataBlock(List<Long> positions, IndexedRecord... records) throws IOException {
    HoodieDataBlock block = mock(HoodieDataBlock.class);
    when(block.getBaseFileInstantTimeOfPositions()).thenReturn(positions == null ? null : "001");
    when(block.getRecordPositionList()).thenReturn(positions == null ? Collections.emptyList() : positions);
    when(block.getSchema()).thenReturn(SCHEMA);
    when(block.getEngineRecordIterator(any(HoodieReaderContext.class)))
        .thenAnswer(invocation -> ClosableIterator.wrap(Arrays.asList(records).iterator()));
    return block;
  }

  /**
   * A delete block for the given keys at the given base file row positions, or without positions when
   * {@code positions} is null, ordered after every record the tests write.
   */
  private static HoodieDeleteBlock deleteBlock(List<Long> positions, String... keys) throws IOException {
    HoodieDeleteBlock block = mock(HoodieDeleteBlock.class);
    when(block.getBaseFileInstantTimeOfPositions()).thenReturn(positions == null ? null : "001");
    when(block.getRecordPositionList()).thenReturn(positions == null ? Collections.emptyList() : positions);
    when(block.getRecordsToDelete(any(HoodieReaderContext.class))).thenAnswer(invocation -> {
      HoodieReaderContext<IndexedRecord> context = invocation.getArgument(0);
      return Arrays.stream(keys)
          .map(key -> BufferedRecords.fromDeleteRecord(DeleteRecord.create(key, "", 3L), context.getRecordContext()))
          .collect(Collectors.toList());
    });
    return block;
  }

  /**
   * Merges the buffer with base rows {@code k0..k(n-1)} at ts 1 and returns the surviving rows as {@code key:ts},
   * sorted.
   */
  private static List<String> mergeWithBase(PositionBasedFileGroupRecordBuffer<IndexedRecord> buffer, int baseRows) throws IOException {
    List<IndexedRecord> base = LongStream.range(0, baseRows).mapToObj(i -> record("k" + i, 1)).collect(Collectors.toList());
    buffer.setBaseFileIterator(ClosableIterator.wrap(base.iterator()));
    List<String> rows = new ArrayList<>();
    while (buffer.hasNext()) {
      rows.add(format(buffer.next().getRecord()));
    }
    buffer.close();
    Collections.sort(rows);
    return rows;
  }

  private static String format(IndexedRecord record) {
    return record.get(0) + ":" + record.get(1);
  }

  private static IndexedRecord record(String key, long ts) {
    GenericData.Record record = new GenericData.Record(SCHEMA.toAvroSchema());
    record.put("id", key);
    record.put("ts", ts);
    return record;
  }
}
