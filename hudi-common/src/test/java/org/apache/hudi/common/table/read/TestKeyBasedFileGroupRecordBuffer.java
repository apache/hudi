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

package org.apache.hudi.common.table.read;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestKeyBasedFileGroupRecordBuffer {
  private static final Schema SCHEMA = Schema.createRecord("test_record", null, "namespace", false,
      Arrays.asList(
          new Schema.Field("record_key", Schema.create(Schema.Type.STRING)),
          new Schema.Field("counter", Schema.create(Schema.Type.INT)),
          new Schema.Field("ts", Schema.create(Schema.Type.LONG))));
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

  @Test
  void readWithEventTimeOrdering() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, null,
        RecordMergeMode.EVENT_TIME_ORDERING, Option.of("ts"), Option.of(Pair.of("counter", "3")));

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
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, null,
        RecordMergeMode.EVENT_TIME_ORDERING, Option.of("ts"), Option.of(Pair.of("counter", "3")));

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3).iterator()));

    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getSchema()).thenReturn(SCHEMA);
    when(dataBlock.getEngineRecordIterator(readerContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord1UpdateWithSameTime, testRecord2Update, testRecord3).iterator()));

    HoodieDataBlock dataBlock2 = mock(HoodieDataBlock.class);
    when(dataBlock2.getSchema()).thenReturn(SCHEMA);
    when(dataBlock2.getEngineRecordIterator(readerContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2EarlierUpdate, testRecord3Update).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[]{DeleteRecord.create("3", ""), DeleteRecord.create("2", "", -1L),
        DeleteRecord.create("1", "", 2L)});
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
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, null,
        RecordMergeMode.COMMIT_TIME_ORDERING, Option.empty(), Option.of(Pair.of("counter", "3")));

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
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, new HoodieAvroRecordMerger(),
        RecordMergeMode.CUSTOM, Option.empty(), Option.empty());

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
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[]{DeleteRecord.create("3", "")});
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
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, new CustomMerger(),
        RecordMergeMode.CUSTOM, Option.empty(), Option.empty());

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
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[]{DeleteRecord.create("3", "")});
    fileGroupRecordBuffer.processDataBlock(dataBlock1, Option.empty());
    fileGroupRecordBuffer.processDataBlock(dataBlock2, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Collections.singletonList(testRecord1), actualRecords);

    assertEquals(0, readStats.getNumInserts());
    assertEquals(3, readStats.getNumDeletes());
    assertEquals(0, readStats.getNumUpdates());
  }

  private static GenericRecord createTestRecord(String recordKey, int counter, long ts) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("record_key", recordKey);
    record.put("counter", counter);
    record.put("ts", ts);
    return record;
  }

  private static KeyBasedFileGroupRecordBuffer<IndexedRecord> buildKeyBasedFileGroupRecordBuffer(HoodieReaderContext<IndexedRecord> readerContext,
                                                                                                 HoodieTableConfig tableConfig,
                                                                                                 HoodieReadStats readStats,
                                                                                                 HoodieRecordMerger recordMerger,
                                                                                                 RecordMergeMode recordMergeMode,
                                                                                                 Option<String> orderingFieldName,
                                                                                                 Option<Pair<String, String>> deleteMarkerKeyValue) {

    readerContext.setRecordMerger(Option.ofNullable(recordMerger));
    FileGroupReaderSchemaHandler<IndexedRecord> fileGroupReaderSchemaHandler = mock(FileGroupReaderSchemaHandler.class);
    when(fileGroupReaderSchemaHandler.getRequiredSchema()).thenReturn(SCHEMA);
    when(fileGroupReaderSchemaHandler.getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(fileGroupReaderSchemaHandler.getCustomDeleteMarkerKeyValue()).thenReturn(deleteMarkerKeyValue);
    when(fileGroupReaderSchemaHandler.getHoodieOperationPos()).thenReturn(-1);
    readerContext.setSchemaHandler(fileGroupReaderSchemaHandler);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(mockMetaClient.getTableConfig()).thenReturn(tableConfig);
    TypedProperties props = new TypedProperties();
    UpdateProcessor<IndexedRecord> updateProcessor = UpdateProcessor.create(readStats, readerContext, false, Option.empty());
    return new KeyBasedFileGroupRecordBuffer<>(
        readerContext, mockMetaClient, recordMergeMode, PartialUpdateMode.NONE, props, orderingFieldName, updateProcessor);
  }

  private static List<IndexedRecord> getActualRecords(FileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer) throws IOException {
    List<IndexedRecord> actualRecords = new ArrayList<>();
    while (fileGroupRecordBuffer.hasNext()) {
      actualRecords.add(fileGroupRecordBuffer.next());
    }
    return actualRecords;
  }

  /**
   * A custom payload implementation for testing purposes that marks records as deleted once the counter exceeds 2.
   * During the merge, it will combine the counter values.
   */
  public static class CustomPayload extends BaseAvroPayload
      implements HoodieRecordPayload<CustomPayload> {
    private final GenericRecord payloadRecord;

    public CustomPayload(GenericRecord record, Comparable orderingVal) {
      super(record, orderingVal);
      this.payloadRecord = record;
    }

    @Override
    public CustomPayload preCombine(CustomPayload oldValue) {
      return this;
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
      if (currentValue.get(2).equals(payloadRecord.get(2))) {
        // If the timestamps are the same, we do not update
        return Option.of(currentValue);
      }
      int result = (int) currentValue.get(1) + (int) payloadRecord.get(1);
      if (result > 2) {
        return Option.empty();
      }
      return Option.of(createTestRecord(currentValue.get(0).toString(), result, (long) payloadRecord.get(2)));
    }

    @Override
    public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
      return Option.of(payloadRecord);
    }

    @Override
    public Comparable<?> getOrderingValue() {
      return null;
    }
  }

  public static class CustomMerger implements HoodieRecordMerger {
    private final String strategy = UUID.randomUUID().toString();

    @Override
    public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
      GenericRecord olderData = (GenericRecord) older.getData();
      GenericRecord newerData = (GenericRecord) newer.getData();
      if (olderData.get(2).equals(newerData.get(2))) {
        // If the timestamps are the same, we do not update
        return Option.of(Pair.of(older, oldSchema));
      }
      int result = (int) olderData.get(1) + (int) newerData.get(1);
      if (result > 2) {
        return Option.empty();
      }
      HoodieKey hoodieKey = older.getKey();
      return Option.of(Pair.of(new HoodieAvroIndexedRecord(createTestRecord(hoodieKey.getRecordKey(), result, (long) newerData.get(2))), SCHEMA));
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType() {
      return HoodieRecord.HoodieRecordType.AVRO;
    }

    @Override
    public String getMergingStrategy() {
      return strategy;
    }
  }
}
