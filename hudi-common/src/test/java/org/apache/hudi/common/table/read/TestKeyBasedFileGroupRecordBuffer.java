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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestKeyBasedFileGroupRecordBuffer {
  private static final Schema SCHEMA = Schema.createRecord("test_record", null, "namespace", false,
      Arrays.asList(new Schema.Field("record_key", Schema.create(Schema.Type.STRING)), new Schema.Field("counter", Schema.create(Schema.Type.INT))));
  private final IndexedRecord testRecord1 = createTestRecord("1", 1);
  private final IndexedRecord testRecord2 = createTestRecord("2", 1);
  private final IndexedRecord testRecord2Update = createTestRecord("2", 1);
  private final IndexedRecord testRecord2Delete = createTestRecord("2", 2);
  private final IndexedRecord testRecord3 = createTestRecord("3", 1);

  @Test
  void readBaseFileAndLogFileWithCustomPayload() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn(CustomPayload.class.getName());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildSortedKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, new HoodieAvroRecordMerger());

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3).iterator()));

    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getSchema()).thenReturn(SCHEMA);
    when(dataBlock.getEngineRecordIterator(readerContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2Update, testRecord2Delete).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[]{DeleteRecord.create("3", "")});
    fileGroupRecordBuffer.processDataBlock(dataBlock, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Collections.singletonList(testRecord1), actualRecords);
  }

  @Test
  void readBaseFileAndLogFileWithCustomMerger() throws IOException {
    HoodieReadStats readStats = new HoodieReadStats();
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getPayloadClass()).thenReturn(CustomPayload.class.getName());
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"record_key"}));
    StorageConfiguration<?> storageConfiguration = mock(StorageConfiguration.class);
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(storageConfiguration, tableConfig, Option.empty(), Option.empty());
    KeyBasedFileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer = buildSortedKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, new CustomMerger());

    fileGroupRecordBuffer.setBaseFileIterator(ClosableIterator.wrap(Arrays.asList(testRecord1, testRecord2, testRecord3).iterator()));

    HoodieDataBlock dataBlock = mock(HoodieDataBlock.class);
    when(dataBlock.getSchema()).thenReturn(SCHEMA);
    when(dataBlock.getEngineRecordIterator(readerContext)).thenReturn(ClosableIterator.wrap(Arrays.asList(testRecord2Update, testRecord2Delete).iterator()));

    HoodieDeleteBlock deleteBlock = mock(HoodieDeleteBlock.class);
    when(deleteBlock.getRecordsToDelete()).thenReturn(new DeleteRecord[]{DeleteRecord.create("3", "")});
    fileGroupRecordBuffer.processDataBlock(dataBlock, Option.empty());
    fileGroupRecordBuffer.processDeleteBlock(deleteBlock);

    List<IndexedRecord> actualRecords = getActualRecords(fileGroupRecordBuffer);
    assertEquals(Collections.singletonList(testRecord1), actualRecords);
  }

  private static GenericRecord createTestRecord(String recordKey, int counter) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("record_key", recordKey);
    record.put("counter", counter);
    return record;
  }

  private static KeyBasedFileGroupRecordBuffer<IndexedRecord> buildSortedKeyBasedFileGroupRecordBuffer(HoodieReaderContext<IndexedRecord> readerContext,
                                                                                                       HoodieTableConfig tableConfig,
                                                                                                       HoodieReadStats readStats,
                                                                                                       HoodieRecordMerger recordMerger) {

    readerContext.setRecordMerger(Option.of(recordMerger));
    FileGroupReaderSchemaHandler<IndexedRecord> fileGroupReaderSchemaHandler = mock(FileGroupReaderSchemaHandler.class);
    when(fileGroupReaderSchemaHandler.getRequiredSchema()).thenReturn(SCHEMA);
    when(fileGroupReaderSchemaHandler.getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(fileGroupReaderSchemaHandler.getCustomDeleteMarkerKeyValue()).thenReturn(Option.empty());
    when(fileGroupReaderSchemaHandler.getHoodieOperationPos()).thenReturn(-1);
    readerContext.setSchemaHandler(fileGroupReaderSchemaHandler);
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(mockMetaClient.getTableConfig()).thenReturn(tableConfig);
    RecordMergeMode recordMergeMode = RecordMergeMode.CUSTOM;
    TypedProperties props = new TypedProperties();
    return new KeyBasedFileGroupRecordBuffer<>(readerContext, mockMetaClient, recordMergeMode, props, readStats, Option.empty(), false);
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
    private final GenericRecord currentRecord;

    public CustomPayload(GenericRecord record, Comparable orderingVal) {
      super(record, orderingVal);
      this.currentRecord = record;
    }

    @Override
    public CustomPayload preCombine(CustomPayload oldValue) {
      return this;
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
      int result = (int) currentValue.get(1) + (int) currentRecord.get(1);
      if (result > 2) {
        return Option.empty();
      }
      return Option.of(createTestRecord(currentValue.get(0).toString(), result));
    }

    @Override
    public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
      return Option.of(currentRecord);
    }

    @Override
    public Comparable<?> getOrderingValue() {
      return null;
    }
  }

  public static class CustomMerger implements HoodieRecordMerger {

    @Override
    public Option<Pair<HoodieRecord, Schema>> merge(HoodieRecord older, Schema oldSchema, HoodieRecord newer, Schema newSchema, TypedProperties props) throws IOException {
      int result = (int) ((GenericRecord) older.getData()).get(1) + (int) ((GenericRecord) newer.getData()).get(1);
      if (result > 2) {
        return Option.empty();
      }
      HoodieKey hoodieKey = older.getKey();
      return Option.of(Pair.of(new HoodieAvroIndexedRecord(createTestRecord(hoodieKey.getRecordKey(), result)), SCHEMA));
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType() {
      return HoodieRecord.HoodieRecordType.AVRO;
    }

    @Override
    public String getMergingStrategy() {
      return "";
    }
  }
}