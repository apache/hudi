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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.table.read.HoodieReadStats;
import org.apache.hudi.common.table.read.InputSplit;
import org.apache.hudi.common.table.read.ReaderParameters;
import org.apache.hudi.common.table.read.UpdateProcessor;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.InternalSchema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseTestFileGroupRecordBuffer {

  protected static final Schema SCHEMA = Schema.createRecord("test_record", null, "namespace", false,
      Arrays.asList(
          new Schema.Field("record_key", Schema.create(Schema.Type.STRING)),
          new Schema.Field("counter", Schema.create(Schema.Type.INT)),
          new Schema.Field("ts", Schema.create(Schema.Type.LONG))));

  protected static GenericRecord createTestRecord(String recordKey, int counter, long ts) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("record_key", recordKey);
    record.put("counter", counter);
    record.put("ts", ts);
    return record;
  }

  protected static List<HoodieRecord> convertToHoodieRecordsList(List<IndexedRecord> indexedRecords) {
    return indexedRecords.stream().map(rec -> new HoodieAvroIndexedRecord(new HoodieKey(rec.get(0).toString(), ""), rec, null)).collect(Collectors.toList());
  }

  protected static List<HoodieRecord> convertToHoodieRecordsListForDeletes(List<IndexedRecord> indexedRecords, boolean defaultOrderingValue) {
    return indexedRecords.stream().map(rec -> new HoodieEmptyRecord<>(new HoodieKey(rec.get(0).toString(), ""),
        HoodieOperation.DELETE, defaultOrderingValue ? 0 : (Comparable) rec.get(2), HoodieRecord.HoodieRecordType.AVRO)).collect(Collectors.toList());
  }

  protected static KeyBasedFileGroupRecordBuffer<IndexedRecord> buildKeyBasedFileGroupRecordBuffer(HoodieReaderContext<IndexedRecord> readerContext,
                                                                                                 HoodieTableConfig tableConfig,
                                                                                                 HoodieReadStats readStats,
                                                                                                 HoodieRecordMerger recordMerger,
                                                                                                 RecordMergeMode recordMergeMode,
                                                                                                 List<String> orderingFieldNames,
                                                                                                 Option<Pair<String, String>> deleteMarkerKeyValue) {
    TypedProperties props = new TypedProperties();
    deleteMarkerKeyValue.ifPresent(markerKeyValue -> {
      props.setProperty(DELETE_KEY, markerKeyValue.getLeft());
      props.setProperty(DELETE_MARKER, markerKeyValue.getRight());
    });
    FileGroupReaderSchemaHandler<IndexedRecord> fileGroupReaderSchemaHandler = mock(FileGroupReaderSchemaHandler.class);
    when(fileGroupReaderSchemaHandler.getRequiredSchema()).thenReturn(SCHEMA);
    when(fileGroupReaderSchemaHandler.getInternalSchema()).thenReturn(InternalSchema.getEmptyInternalSchema());
    when(fileGroupReaderSchemaHandler.getDeleteContext()).thenReturn(new DeleteContext(props, SCHEMA));
    readerContext.setSchemaHandler(fileGroupReaderSchemaHandler);
    return buildKeyBasedFileGroupRecordBuffer(readerContext, tableConfig, readStats, recordMerger, recordMergeMode, orderingFieldNames, props,
        Option.empty());
  }

  protected static KeyBasedFileGroupRecordBuffer<IndexedRecord> buildKeyBasedFileGroupRecordBuffer(HoodieReaderContext<IndexedRecord> readerContext,
                                                                                                 HoodieTableConfig tableConfig,
                                                                                                 HoodieReadStats readStats,
                                                                                                 HoodieRecordMerger recordMerger,
                                                                                                 RecordMergeMode recordMergeMode,
                                                                                                 List<String> orderingFieldNames,
                                                                                                 TypedProperties props,
                                                                                                 Option<Iterator<HoodieRecord>> fileGroupRecordBufferItrOpt) {

    readerContext.setRecordMerger(Option.ofNullable(recordMerger));
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    when(mockMetaClient.getTableConfig()).thenReturn(tableConfig);
    UpdateProcessor<IndexedRecord> updateProcessor = UpdateProcessor.create(readStats, readerContext, false, Option.empty(), props);

    if (fileGroupRecordBufferItrOpt.isEmpty()) {
      return new KeyBasedFileGroupRecordBuffer<>(
          readerContext, mockMetaClient, recordMergeMode, Option.empty(), props, orderingFieldNames, updateProcessor);
    } else {
      FileGroupRecordBufferLoader recordBufferLoader = FileGroupRecordBufferLoader.createStreamingRecordsBufferLoader();
      InputSplit inputSplit = mock(InputSplit.class);
      when(inputSplit.hasNoRecordsToMerge()).thenReturn(false);
      when(inputSplit.getRecordIterator()).thenReturn(fileGroupRecordBufferItrOpt.get());
      ReaderParameters readerParameters = mock(ReaderParameters.class);
      when(readerParameters.sortOutputs()).thenReturn(false);
      return (KeyBasedFileGroupRecordBuffer<IndexedRecord>) recordBufferLoader.getRecordBuffer(readerContext, mockMetaClient.getStorage(), inputSplit,
          orderingFieldNames, mockMetaClient, props, readerParameters, readStats, Option.empty()).getKey();
    }
  }

  protected static List<IndexedRecord> getActualRecords(FileGroupRecordBuffer<IndexedRecord> fileGroupRecordBuffer) throws IOException {
    List<IndexedRecord> actualRecords = new ArrayList<>();
    while (fileGroupRecordBuffer.hasNext()) {
      actualRecords.add(fileGroupRecordBuffer.next().getRecord());
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
    public TestKeyBasedFileGroupRecordBuffer.CustomPayload preCombine(TestKeyBasedFileGroupRecordBuffer.CustomPayload oldValue) {
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
    public Option<IndexedRecord> getIndexedRecord(Schema schema, Properties properties) {
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
