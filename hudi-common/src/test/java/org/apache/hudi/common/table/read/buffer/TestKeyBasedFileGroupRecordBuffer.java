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
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestKeyBasedFileGroupRecordBuffer extends BaseTestFileGroupRecordBuffer {
  private final IndexedRecord testRecord1 = createTestRecord("1", 1, 1L);
  private final IndexedRecord testRecord1UpdateWithSameTime = createTestRecord("1", 2, 1L);
  private final IndexedRecord testRecord2 = createTestRecord("2", 1, 1L);
  private final IndexedRecord testRecord2Update = createTestRecord("2", 1, 2L);
  private final IndexedRecord testRecord2EarlierUpdate = createTestRecord("2", 1, 0L);
  private final IndexedRecord testRecord2Delete = createTestRecord("2", 2, 3L);
  private final IndexedRecord testRecord3 = createTestRecord("3", 1, 1L);
  private final IndexedRecord testRecord3Update = createTestRecord("3", 1, 2L);
  private final IndexedRecord testRecord3DeleteByFieldValue = createTestRecord("3", 3, 1L);
  private final IndexedRecord testRecord4 = createTestRecord("4", 2, 1L);
  private final IndexedRecord testRecord4Update = createTestRecord("4", 1, 2L);

  /**
   * Asserts that {@code processNextDataRecord} and {@code isPartialMergingEnabled} keep their
   * {@code final} modifier, so a subclass cannot substitute its own implementation of either.
   *
   * <p>That is the entire guarantee, and it is deliberately narrow. It does <em>not</em> mean all buffer
   * mutations go through {@code processNextDataRecord}: {@code records} and {@code enablePartialMerging}
   * are {@code protected}, and {@code PositionBasedFileGroupRecordBuffer} legitimately writes to
   * {@code records} directly when it re-keys entries in its key-based fallback and when it overwrites
   * delete markers under commit-time ordering, as well as overriding block processing. Those paths must
   * not merge, so routing them through this method would be wrong.
   *
   * <p>The modifier itself is compiler-enforced; this test exists only to catch it being dropped.
   */
  @Test
  void sealedMethodsCannotBeOverridden() throws NoSuchMethodException {
    assertMethodIsFinal(KeyBasedFileGroupRecordBuffer.class
        .getDeclaredMethod("processNextDataRecord", BufferedRecord.class, Serializable.class));
    assertMethodIsFinal(KeyBasedFileGroupRecordBuffer.class.getDeclaredMethod("isPartialMergingEnabled"));
  }

  private static void assertMethodIsFinal(Method method) {
    assertTrue(Modifier.isFinal(method.getModifiers()),
        () -> String.format("%s#%s must remain final so subclasses cannot override it",
            method.getDeclaringClass().getSimpleName(), method.getName()));
  }

  @Test
  void readWithEventTimeOrdering() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.empty());
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, null,
        RecordMergeMode.EVENT_TIME_ORDERING, Collections.singletonList("ts"), Option.of(Pair.of("counter", "3")));

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3).iterator()));

    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getSchema()).thenReturn(SCHEMA);
    when(dataBlock.getEngineRecordIterator(readerContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Update, testRecord2EarlierUpdate,
        testRecord3Update, testRecord3DeleteByFieldValue).iterator()));

    fileGroupRecordBuffer.processDataBlock(dataBlock, Option.empty());

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    // delete for record3 is ignored due to event time ordering
    assertEquals(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Update, testRecord3Update), actualRecords);
    assertEquals(0, readStats.getNumInserts());
    assertEquals(0, readStats.getNumDeletes());
    assertEquals(3, readStats.getNumUpdates());
  }

  @Test
  void readWithEventTimeOrderingAndDeleteBlock() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.empty());
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, null,
        RecordMergeMode.EVENT_TIME_ORDERING, Collections.singletonList("ts"), Option.of(Pair.of("counter", "3")));

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3).iterator()));

    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getSchema()).thenReturn(SCHEMA);
    when(dataBlock.getEngineRecordIterator(readerContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Update, testRecord3).iterator()));

    HoodieDataBlock dataBlock2 = mock(HoodieDataBlock.class);
    when(dataBlock2.getSchema()).thenReturn(SCHEMA);
    when(dataBlock2.getEngineRecordIterator(readerContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2EarlierUpdate, testRecord3Update).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    mockDeleteRecords(deleteBlock, DeleteRecord.create("3", ""), DeleteRecord.create("2", "", -1L), DeleteRecord.create("1", "", 2L));
    // process data block, then delete block, then another data block
    fileGroupRecordBuffer.processDataBlock(dataBlock, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);
    fileGroupRecordBuffer.processDataBlock(dataBlock2, Option.empty());

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Arrays.asList(testRecord2Update, testRecord3Update), actualRecords);
    assertEquals(0, readStats.getNumInserts());
    assertEquals(1, readStats.getNumDeletes());
    assertEquals(2, readStats.getNumUpdates());
  }

  @Test
  void readWithCommitTimeOrdering() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.empty());
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, null,
        RecordMergeMode.COMMIT_TIME_ORDERING, Collections.emptyList(), Option.of(Pair.of("counter", "3")));

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3).iterator()));

    HoodieDataBlock dataBlock1 = mock(HoodieDataBlock.class);
    when(dataBlock1.getSchema()).thenReturn(SCHEMA);
    when(dataBlock1.getEngineRecordIterator(readerContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Update, testRecord2EarlierUpdate).iterator()));

    HoodieDataBlock dataBlock2 = mock(HoodieDataBlock.class);
    when(dataBlock2.getSchema()).thenReturn(SCHEMA);
    when(dataBlock2.getEngineRecordIterator(readerContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2EarlierUpdate, testRecord3Update, testRecord3DeleteByFieldValue).iterator()));

    fileGroupRecordBuffer.processDataBlock(dataBlock1, Option.empty());
    fileGroupRecordBuffer.processDataBlock(dataBlock2, Option.empty());

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2EarlierUpdate), actualRecords);
    assertEquals(0, readStats.getNumInserts());
    assertEquals(1, readStats.getNumDeletes());
    assertEquals(2, readStats.getNumUpdates());
  }

  @Test
  void readWithCustomPayload() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn(CustomPayload.class.getName());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.empty());
    when(tableConfig.getRecordMergeMode()).thenReturn(RecordMergeMode.CUSTOM);
    when(tableConfig.getPartialUpdateMode()).thenReturn(Option.empty());
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, new HoodieAvroRecordMerger(),
        RecordMergeMode.CUSTOM, Collections.emptyList(), Option.empty());

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3, testRecord4).iterator()));

    HoodieDataBlock dataBlock1 = mock(HoodieDataBlock.class);
    when(dataBlock1.getSchema()).thenReturn(SCHEMA);
    when(dataBlock1.getEngineRecordIterator(readerContext))
        .thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2Update, testRecord1UpdateWithSameTime).iterator()));

    HoodieDataBlock dataBlock2 = mock(HoodieDataBlock.class);
    when(dataBlock2.getSchema()).thenReturn(SCHEMA);
    when(dataBlock2.getEngineRecordIterator(readerContext))
        .thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Delete, testRecord4Update).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    mockDeleteRecords(deleteBlock, DeleteRecord.create("3", ""));
    fileGroupRecordBuffer.processDataBlock(dataBlock1, Option.empty());
    fileGroupRecordBuffer.processDataBlock(dataBlock2, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Collections.singletonList(testRecord1), actualRecords);

    assertEquals(0, readStats.getNumInserts());
    assertEquals(3, readStats.getNumDeletes());
    assertEquals(0, readStats.getNumUpdates());
  }

  @Test
  void readWithCustomMerger() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn(CustomPayload.class.getName());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[] {"record_key"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.empty());
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, new CustomMerger(),
        RecordMergeMode.CUSTOM, Collections.emptyList(), Option.empty());

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3, testRecord4).iterator()));

    HoodieDataBlock dataBlock1 = mock(HoodieDataBlock.class);
    when(dataBlock1.getSchema()).thenReturn(SCHEMA);
    when(dataBlock1.getEngineRecordIterator(readerContext))
        .thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2Update, testRecord1UpdateWithSameTime, testRecord1UpdateWithSameTime).iterator()));

    HoodieDataBlock dataBlock2 = mock(HoodieDataBlock.class);
    when(dataBlock2.getSchema()).thenReturn(SCHEMA);
    when(dataBlock2.getEngineRecordIterator(readerContext))
        .thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2Delete, testRecord4Update).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    mockDeleteRecords(deleteBlock, DeleteRecord.create("3", ""));
    fileGroupRecordBuffer.processDataBlock(dataBlock1, Option.empty());
    fileGroupRecordBuffer.processDataBlock(dataBlock2, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Collections.singletonList(testRecord1), actualRecords);

    assertEquals(0, readStats.getNumInserts());
    assertEquals(3, readStats.getNumDeletes());
    assertEquals(0, readStats.getNumUpdates());
  }
}
