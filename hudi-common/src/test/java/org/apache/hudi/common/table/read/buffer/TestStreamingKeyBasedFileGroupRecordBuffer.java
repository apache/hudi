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
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestStreamingKeyBasedFileGroupRecordBuffer extends BaseTestFileGroupRecordBuffer {
  private final IndexedRecord testRecord1 = createTestRecord("1", 1, 1L);
  private final IndexedRecord testRecord1UpdateWithSameTime = createTestRecord("1", 2, 1L);
  private final IndexedRecord testRecord2 = createTestRecord("2", 1, 1L);
  private final IndexedRecord testRecord2Update = createTestRecord("2", 1, 2L);
  private final IndexedRecord testRecord2CustomPayloadExpected = createTestRecord("2", 2, 2L);
  private final IndexedRecord testRecord3 = createTestRecord("3", 1, 1L);
  private final IndexedRecord testRecord3Update = createTestRecord("3", 1, 2L);
  private final IndexedRecord testRecord3UpdateCustomPayloadExpected = createTestRecord("3", 2, 2L);
  private final IndexedRecord testRecord4 = createTestRecord("4", 2, 1L);
  private final IndexedRecord testRecord4EarlierUpdate = createTestRecord("4", 1, 0L);
  private final IndexedRecord testRecord5 = createTestRecord("5", 1, 1L);
  private final IndexedRecord testRecord5DeleteByCustomMarker = createTestRecord("5", 3, 2L);
  private final IndexedRecord testRecord6 = createTestRecord("6", 1, 5L);
  private final IndexedRecord testRecord6DeleteByCustomMarker = createTestRecord("6", 3, 2L);
  private final IndexedRecord testRecord7 = createTestRecord("7", 1, 5L);

  @Test
  void readWithEventTimeOrdering() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    TypedProperties properties = new TypedProperties();
    properties.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    properties.setProperty(DELETE_KEY, "counter");
    properties.setProperty(DELETE_MARKER, "3");
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.EVENT_TIME_ORDERING);
    when(tableConfig.getPartialUpdateMode()).thenReturn(Option.empty());
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    readerContext.setHasLogFiles(false);
    readerContext.setHasBootstrapBaseFile(false);
    readerContext.initRecordMerger(properties);
    FileGroupReaderSchemaHandler schemaHandler = new FileGroupReaderSchemaHandler(readerContext, SCHEMA, SCHEMA, Option.empty(),
        properties, mock(HoodieTableMetaClient.class));
    readerContext.setSchemaHandler(schemaHandler);
    List<HoodieRecord> inputRecords = convertToHoodieRecordsList(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Update, testRecord3Update, testRecord4EarlierUpdate, testRecord7));
    inputRecords.addAll(convertToHoodieRecordsListForDeletes(Arrays.asList(testRecord5DeleteByCustomMarker, testRecord6DeleteByCustomMarker), false));
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, null,
        RecordMergeMode.EVENT_TIME_ORDERING, Collections.singletonList("ts"), properties, Option.of(inputRecords.iterator()));

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3, testRecord4,
        testRecord5, testRecord6).iterator()));

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    // update for 4 is ignored due to lower ordering value.
    // record5 is deleted.
    // delete for 6 is ignored due to lower ordering value.
    assertEquals(Arrays.asList(getSerializableIndexedRecord(testRecord1UpdateWithSameTime), getSerializableIndexedRecord(testRecord2Update),
        getSerializableIndexedRecord(testRecord3Update), testRecord4, testRecord6, getSerializableIndexedRecord(testRecord7)), actualRecords);
    assertEquals(1, readStats.getNumInserts());
    assertEquals(1, readStats.getNumDeletes());
    assertEquals(3, readStats.getNumUpdates());
  }

  @Test
  void readWithCommitTimeOrdering() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    TypedProperties properties = new TypedProperties();
    properties.setProperty(DELETE_KEY, "counter");
    properties.setProperty(DELETE_MARKER, "3");
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.COMMIT_TIME_ORDERING);
    when(tableConfig.getPartialUpdateMode()).thenReturn(Option.empty());
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    readerContext.setHasLogFiles(false);
    readerContext.setHasBootstrapBaseFile(false);
    readerContext.initRecordMerger(properties);
    FileGroupReaderSchemaHandler schemaHandler = new FileGroupReaderSchemaHandler(readerContext, SCHEMA, SCHEMA, Option.empty(),
        properties, mock(HoodieTableMetaClient.class));
    readerContext.setSchemaHandler(schemaHandler);
    List<HoodieRecord> inputRecords = convertToHoodieRecordsList(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Update, testRecord3Update, testRecord4EarlierUpdate, testRecord7));
    inputRecords.addAll(convertToHoodieRecordsListForDeletes(Arrays.asList(testRecord5DeleteByCustomMarker, testRecord6DeleteByCustomMarker), true));
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, null,
        RecordMergeMode.COMMIT_TIME_ORDERING, Collections.singletonList("ts"), properties, Option.of(inputRecords.iterator()));

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3, testRecord4,
        testRecord5, testRecord6).iterator()));

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Arrays.asList(getSerializableIndexedRecord(testRecord1UpdateWithSameTime), getSerializableIndexedRecord(testRecord2Update),
        getSerializableIndexedRecord(testRecord3Update), getSerializableIndexedRecord(testRecord4EarlierUpdate), getSerializableIndexedRecord(testRecord7)),
        actualRecords);
    assertEquals(1, readStats.getNumInserts());
    assertEquals(2, readStats.getNumDeletes());
    assertEquals(4, readStats.getNumUpdates());
  }

  @Test
  void readWithCustomPayload() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    TypedProperties properties = new TypedProperties();
    properties.setProperty(DELETE_KEY, "counter");
    properties.setProperty(DELETE_MARKER, "3");
    properties.setProperty(HoodieTableConfig.RECORD_MERGE_MODE.key(), "CUSTOM");
    properties.setProperty(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), CustomPayload.class.getName());
    properties.setProperty(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key(), HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn(CustomPayload.class.getName());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.CUSTOM);
    when(tableConfig.getPartialUpdateMode()).thenReturn(Option.empty());
    when(tableConfig.getRecordMergeStrategyId()).thenReturn(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID);
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    readerContext.setHasLogFiles(false);
    readerContext.setHasBootstrapBaseFile(false);
    readerContext.initRecordMerger(properties);
    FileGroupReaderSchemaHandler schemaHandler = new FileGroupReaderSchemaHandler(readerContext, SCHEMA, SCHEMA, Option.empty(),
        properties, mock(HoodieTableMetaClient.class));
    readerContext.setSchemaHandler(schemaHandler);
    List<HoodieRecord> inputRecords = convertToHoodieRecordsList(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Update, testRecord3Update, testRecord4EarlierUpdate));
    inputRecords.addAll(convertToHoodieRecordsListForDeletes(Arrays.asList(testRecord5DeleteByCustomMarker, testRecord6DeleteByCustomMarker), true));
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, new HoodieAvroRecordMerger(),
        RecordMergeMode.CUSTOM, Collections.singletonList("ts"), properties, Option.of(inputRecords.iterator()));

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3, testRecord4,
        testRecord5, testRecord6).iterator()));

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Arrays.asList(testRecord1, testRecord2CustomPayloadExpected, testRecord3UpdateCustomPayloadExpected), actualRecords);
    assertEquals(0, readStats.getNumInserts());
    assertEquals(3, readStats.getNumDeletes());
    assertEquals(2, readStats.getNumUpdates());
  }

  @Test
  void readWithCustomMergerWithRecords() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    TypedProperties properties = new TypedProperties();
    properties.setProperty(DELETE_KEY, "counter");
    properties.setProperty(DELETE_MARKER, "3");
    properties.setProperty(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(), CustomPayload.class.getName());
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn(CustomPayload.class.getName());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.CUSTOM);
    when(tableConfig.getPartialUpdateMode()).thenReturn(Option.empty());
    when(tableConfig.getRecordMergeStrategyId()).thenReturn(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID);
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());

    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    readerContext.setHasLogFiles(false);
    readerContext.setHasBootstrapBaseFile(false);
    readerContext.initRecordMerger(properties);
    FileGroupReaderSchemaHandler schemaHandler = new FileGroupReaderSchemaHandler(readerContext, SCHEMA, SCHEMA, Option.empty(),
        properties, mock(HoodieTableMetaClient.class));
    readerContext.setSchemaHandler(schemaHandler);
    List<HoodieRecord> inputRecords = convertToHoodieRecordsList(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Update, testRecord3Update, testRecord4EarlierUpdate));
    inputRecords.addAll(convertToHoodieRecordsListForDeletes(Arrays.asList(testRecord5DeleteByCustomMarker, testRecord6DeleteByCustomMarker), true));
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, new TestKeyBasedFileGroupRecordBuffer.CustomMerger(),
        RecordMergeMode.CUSTOM, Collections.singletonList("ts"), properties, Option.of(inputRecords.iterator()));

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3, testRecord4,
        testRecord5, testRecord6).iterator()));

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Arrays.asList(testRecord1, testRecord2CustomPayloadExpected, testRecord3UpdateCustomPayloadExpected), actualRecords);
    assertEquals(0, readStats.getNumInserts());
    assertEquals(3, readStats.getNumDeletes());
    assertEquals(2, readStats.getNumUpdates());
  }
}
