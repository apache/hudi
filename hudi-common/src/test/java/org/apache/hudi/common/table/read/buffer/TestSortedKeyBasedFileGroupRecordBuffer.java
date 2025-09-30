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

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestSortedKeyBasedFileGroupRecordBuffer extends BaseTestFileGroupRecordBuffer {
  private final TestRecord testRecord1 = new TestRecord("1", 0);
  private final TestRecord testRecord2 = new TestRecord("2", 0);
  private final TestRecord testRecord2Update = new TestRecord("2", 1);
  private final TestRecord testRecord3 = new TestRecord("3", 0);
  private final TestRecord testRecord4 = new TestRecord("4", 0);
  private final TestRecord testRecord5 = new TestRecord("5", 0);
  private final TestRecord testRecord6 = new TestRecord("6", 0);
  private final TestRecord testRecord6Update = new TestRecord("6", 1);

  private final IndexedRecord testIndexedRecord1 = createTestRecord("1", 1, 1L);
  private final IndexedRecord testIndexedRecord2 = createTestRecord("2", 1, 1L);
  private final IndexedRecord testIndexedRecord2Update = createTestRecord("2", 1, 2L);
  private final IndexedRecord testIndexedRecord3 = createTestRecord("3", 1, 1L);
  private final IndexedRecord testIndexedRecord4 = createTestRecord("4", 2, 2L);
  private final IndexedRecord testIndexedRecord4LowerOrdering = createTestRecord("4", 2, 1L);
  private final IndexedRecord testIndexedRecord5 = createTestRecord("5", 1, 1L);
  private final IndexedRecord testRecord5DeleteByCustomMarker = createTestRecord("5", 3, 2L);
  private final IndexedRecord testIndexedRecord6 = createTestRecord("6", 1, 5L);
  private final IndexedRecord testIndexedRecord6Update = createTestRecord("6", 2, 10L);

  @Test
  void readBaseFileAndLogFile() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieReaderContext<TestRecord> mockReaderContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);

    SortedKeyBasedFileGroupRecordBuffer<TestRecord> fileGroupRecordBuffer = buildSortedKeyBasedFileGroupRecordBuffer(mockReaderContext, readStats);

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord2, testRecord3, testRecord5).iterator()));

    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(dataBlock.getEngineRecordIterator(mockReaderContext)).thenReturn(
        ClosableIterator.wrap(Arrays.asList(testRecord6, testRecord4, testRecord1, testRecord6Update, testRecord2Update).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[] {DeleteRecord.create("3", "")});
    fileGroupRecordBuffer.processDataBlock(dataBlock, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);

    List<TestRecord> actualRecords = getActualRecordsForSortedKeyBased(fileGroupRecordBuffer);
    assertEquals(Arrays.asList(testRecord1, testRecord2Update, testRecord4, testRecord5, testRecord6Update), actualRecords);
    assertEquals(3, readStats.getNumInserts());
    assertEquals(1, readStats.getNumUpdates());
    assertEquals(1, readStats.getNumDeletes());
  }

  @Test
  void readWithStreamingRecordBufferLoaderAndEventTimeOrdering() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    TypedProperties properties = new TypedProperties();
    properties.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    properties.setProperty(DELETE_KEY, "counter");
    properties.setProperty(DELETE_MARKER, "3");
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    readerContext.setHasLogFiles(false);
    readerContext.setHasBootstrapBaseFile(false);
    FileGroupReaderSchemaHandler schemaHandler = new FileGroupReaderSchemaHandler(readerContext, SCHEMA, SCHEMA, Option.empty(),
        properties, mock(HoodieTableMetaClient.class));
    readerContext.setSchemaHandler(schemaHandler);
    readerContext.initRecordMerger(properties);
    List<HoodieRecord> inputRecords =
        convertToHoodieRecordsList(Arrays.asList(testIndexedRecord6Update, testIndexedRecord4LowerOrdering, testIndexedRecord1, testIndexedRecord2Update));
    inputRecords.addAll(convertToHoodieRecordsListForDeletes(Arrays.asList(testRecord5DeleteByCustomMarker), false));
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(mockMetaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getPayloadClass()).thenReturn(DefaultHoodieRecordPayload.class.getName());
    when(tableConfig.getPartialUpdateMode()).thenReturn(Option.empty());

    FileGroupRecordBufferLoader recordBufferLoader = FileGroupRecordBufferLoader.createStreamingRecordsBufferLoader();
    InputSplit inputSplit = mock(InputSplit.class);
    when(inputSplit.hasNoRecordsToMerge()).thenReturn(false);
    when(inputSplit.getRecordIterator()).thenReturn(inputRecords.iterator());
    ReaderParameters readerParameters = mock(ReaderParameters.class);
    when(readerParameters.sortOutputs()).thenReturn(true);
    SortedKeyBasedFileGroupRecordBuffer fileGroupRecordBuffer  = (SortedKeyBasedFileGroupRecordBuffer<IndexedRecord>) recordBufferLoader
        .getRecordBuffer(readerContext, mockMetaClient.getStorage(), inputSplit, Collections.singletonList("ts"), mockMetaClient, properties,
            readerParameters, readStats, Option.empty()).getKey();
    when(tableConfig.getPayloadClass()).thenReturn(DefaultHoodieRecordPayload.class.getName());

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testIndexedRecord2, testIndexedRecord3, testIndexedRecord4,
        testIndexedRecord5, testIndexedRecord6).iterator()));

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(convertGenRecordsToSerializableIndexedRecords(Stream.of(testIndexedRecord1, testIndexedRecord2Update,
        testIndexedRecord3, testIndexedRecord4, testIndexedRecord6Update)), actualRecords);
    assertEquals(1, readStats.getNumInserts());
    assertEquals(1, readStats.getNumDeletes());
    assertEquals(2, readStats.getNumUpdates());
  }

  @Test
  void readLogFiles() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieReaderContext<TestRecord> mockReaderContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);
    SortedKeyBasedFileGroupRecordBuffer<TestRecord> fileGroupRecordBuffer = buildSortedKeyBasedFileGroupRecordBuffer(mockReaderContext, readStats);

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Collections.emptyIterator()));

    HoodieDataBlock dataBlock1 = mock(HoodieDataBlock.class);
    when(dataBlock1.getSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(dataBlock1.getEngineRecordIterator(mockReaderContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord6, testRecord4, testRecord6Update, testRecord2).iterator()));

    HoodieDataBlock dataBlock2 = mock(HoodieDataBlock.class);
    when(dataBlock2.getSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(dataBlock2.getEngineRecordIterator(mockReaderContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2Update, testRecord5, testRecord3, testRecord1).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[] {DeleteRecord.create("3", "")});
    fileGroupRecordBuffer.processDataBlock(dataBlock1, Option.empty());
    fileGroupRecordBuffer.processDataBlock(dataBlock2, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);


    List<TestRecord> actualRecords = getActualRecordsForSortedKeyBased(fileGroupRecordBuffer);
    assertEquals(Arrays.asList(testRecord1, testRecord2Update, testRecord4, testRecord5, testRecord6Update), actualRecords);
    assertEquals(5, readStats.getNumInserts());
    assertEquals(0, readStats.getNumUpdates());
    assertEquals(1, readStats.getNumDeletes());
  }

  private SortedKeyBasedFileGroupRecordBuffer<TestRecord> buildSortedKeyBasedFileGroupRecordBuffer(HoodieReaderContext<TestRecord> mockReaderContext, HoodieReadStats readStats) {
    when(mockReaderContext.getSchemaHandler().getRequiredSchema()).thenReturn(HoodieTestDataGenerator.AVRO_SCHEMA);
    when(mockReaderContext.getSchemaHandler().getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(mockReaderContext.getRecordContext().getDeleteRow(any())).thenAnswer(invocation -> {
      String recordKey = invocation.getArgument(0);
      return new TestRecord(recordKey, 0);
    });
    when(mockReaderContext.getRecordContext().getRecordKey(any(), any())).thenAnswer(invocation -> ((TestRecord) invocation.getArgument(0)).getRecordKey());
    when(mockReaderContext.getRecordContext().getOrderingValue(any(), any(), anyList())).thenReturn(0);
    when(mockReaderContext.getRecordContext().toBinaryRow(any(), any())).thenAnswer(invocation -> invocation.getArgument(1));
    when(mockReaderContext.getRecordContext().seal(any())).thenAnswer(invocation -> invocation.getArgument(0));
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    RecordMergeMode recordMergeMode = RecordMergeMode.COMMIT_TIME_ORDERING;
    Option<PartialUpdateMode> partialUpdateModeOpt = Option.empty();
    TypedProperties props = new TypedProperties();
    when(mockReaderContext.getPayloadClasses(any())).thenReturn(Option.empty());
    UpdateProcessor<TestRecord> updateProcessor = UpdateProcessor.create(readStats, mockReaderContext, false, Option.empty(), props);
    return new SortedKeyBasedFileGroupRecordBuffer<>(
        mockReaderContext, mockMetaClient, recordMergeMode, partialUpdateModeOpt, props, Collections.emptyList(), updateProcessor);
  }

  private static <T> List<T> getActualRecordsForSortedKeyBased(SortedKeyBasedFileGroupRecordBuffer<T> fileGroupRecordBuffer) throws IOException {
    List<T> actualRecords = new ArrayList<>();
    while (fileGroupRecordBuffer.hasNext()) {
      actualRecords.add(fileGroupRecordBuffer.next().getRecord());
    }
    return actualRecords;
  }
}