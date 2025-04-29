/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestEngineBasedMerger {
  private static final BufferedRecord<TestRecord> T1 = new BufferedRecord<>("key", 1, new TestRecord(), 2, false);
  private static final BufferedRecord<TestRecord> T2 = new BufferedRecord<>("key", 2, new TestRecord(), 2, false);
  private static final BufferedRecord<TestRecord> T3 = new BufferedRecord<>("key", 3, new TestRecord(), 2, false);
  private static final BufferedRecord<TestRecord> HARD_DELETE = new BufferedRecord<>("key", 0, new TestRecord(), 2, true);
  private static final BufferedRecord<TestRecord> T2_SOFT_DELETE = new BufferedRecord<>("key", 2, new TestRecord(), 2, true);

  private final HoodieReaderContext<TestRecord> readerContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);
  private final TypedProperties props = mock(TypedProperties.class);
  private final Schema readerSchema = mock(Schema.class);

  private static Stream<Arguments> commitTimeOrdering() {
    return Stream.of(
        // Validate commit time does not impact the ordering
        Arguments.of(Arrays.asList(T1, T3, T2), T2),
        // Validate hard delete does not impact the ordering
        Arguments.of(Arrays.asList(T1, HARD_DELETE, T2), T2));
  }

  @ParameterizedTest
  @MethodSource
  void commitTimeOrdering(List<BufferedRecord<TestRecord>> recordSequence, BufferedRecord<TestRecord> expected) throws Exception {
    validateSequence(recordSequence, expected, RecordMergeMode.COMMIT_TIME_ORDERING);
  }

  private static Stream<Arguments> eventTimeOrdering() {
    return Stream.of(
        // Validate event time is used
        Arguments.of(Arrays.asList(T1, T3, T2), T3),
        // Validate hard delete is seen as most recent
        Arguments.of(Arrays.asList(T1, HARD_DELETE, T2), HARD_DELETE),
        // Validate soft delete is considered in order
        Arguments.of(Arrays.asList(T1, T2_SOFT_DELETE, T3), T3),
        Arguments.of(Arrays.asList(T3, T2_SOFT_DELETE, T1), T3));
  }

  @ParameterizedTest
  @MethodSource
  void eventTimeOrdering(List<BufferedRecord<TestRecord>> recordSequence, BufferedRecord<TestRecord> expected) throws Exception {
    validateSequence(recordSequence, expected, RecordMergeMode.EVENT_TIME_ORDERING);
  }

  private void validateSequence(List<BufferedRecord<TestRecord>> recordSequence, BufferedRecord<TestRecord> expected, RecordMergeMode recordMergeMode) throws IOException {
    mockEmptyMergerAndSchema();
    EngineBasedMerger<TestRecord> merger = new EngineBasedMerger<>(readerContext, recordMergeMode, null, props);

    BufferedRecord<TestRecord> result = recordSequence.get(0);
    for (int i = 1; i < recordSequence.size(); i++) {
      BufferedRecord<TestRecord> current = recordSequence.get(i);
      result = merger.merge(Option.of(result), Option.of(current), false);
    }
    assertSame(expected, result);
  }

  @Test
  void onlyNewRecordSet() throws Exception {
    mockEmptyMergerAndSchema();
    EngineBasedMerger<TestRecord> merger = new EngineBasedMerger<>(readerContext, RecordMergeMode.COMMIT_TIME_ORDERING, null, props);

    BufferedRecord<TestRecord> result = merger.merge(Option.empty(), Option.of(T1), false);
    assertSame(T1, result);
  }

  @Test
  void onlyOldRecordSet() throws Exception {
    mockEmptyMergerAndSchema();
    EngineBasedMerger<TestRecord> merger = new EngineBasedMerger<>(readerContext, RecordMergeMode.COMMIT_TIME_ORDERING, null, props);

    BufferedRecord<TestRecord> result = merger.merge(Option.of(T1), Option.empty(), false);
    assertSame(T1, result);
  }

  @ParameterizedTest
  @CsvSource({
      "true, true",
      "false, false",
      "true, false",
      "false, true"
  })
  void customMerger(boolean resultIsADelete, boolean usePayload) throws Exception {
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    when(readerContext.getRecordMerger()).thenReturn(Option.of(recordMerger));
    when(readerContext.getSchemaHandler().getRequiredSchema()).thenReturn(readerSchema);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    if (usePayload) {
      when(recordMerger.getMergingStrategy()).thenReturn(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID);
      when(tableConfig.getPayloadClass()).thenReturn(CustomPayloadForTesting.class.getName());
    } else {
      when(recordMerger.getMergingStrategy()).thenReturn(HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID);
    }
    EngineBasedMerger<TestRecord> merger = new EngineBasedMerger<>(readerContext, RecordMergeMode.CUSTOM, tableConfig, props);

    // Mock conversion to HoodieRecords
    Schema olderRecorderSchema = mock(Schema.class);
    Schema newerRecorderSchema = mock(Schema.class);
    HoodieRecord olderRecord = mock(HoodieRecord.class);
    HoodieRecord newerRecord = mock(HoodieRecord.class);
    mockRecordConversion(olderRecorderSchema, newerRecorderSchema, olderRecord, newerRecord);

    // Mock result
    HoodieRecord rewrittenRecord = mock(HoodieRecord.class);
    mockMergeCallAndRewriteWithReaderSchema(recordMerger, olderRecord, olderRecorderSchema, newerRecord, newerRecorderSchema, rewrittenRecord);

    // Mock the result
    TestRecord data = new TestRecord();
    Integer schemaId = 2;
    long orderingValue = 1L;
    String recordKey = "key";
    mockResultConversionToBufferedRecord(schemaId, rewrittenRecord, orderingValue, recordKey, data, resultIsADelete);


    BufferedRecord<TestRecord> result = merger.merge(Option.of(T1), Option.of(T2), false);
    BufferedRecord<TestRecord> expected = new BufferedRecord<>(recordKey, orderingValue, data, schemaId, resultIsADelete);
    assertEquals(expected, result);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
      HoodieRecordMerger.CUSTOM_MERGE_STRATEGY_UUID,
      HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID})
  void customMerger_emptyMergeResult(String mergingStrategy) throws Exception {
    HoodieRecordMerger recordMerger = mock(HoodieRecordMerger.class);
    when(readerContext.getRecordMerger()).thenReturn(Option.of(recordMerger));
    when(readerContext.getSchemaHandler().getRequiredSchema()).thenReturn(readerSchema);
    when(recordMerger.getMergingStrategy()).thenReturn(mergingStrategy);

    // Mock conversion to HoodieRecords, T2 is provided as the "older" record
    Schema olderRecorderSchema = mock(Schema.class);
    Schema newerRecorderSchema = mock(Schema.class);
    HoodieRecord olderRecord = mock(HoodieRecord.class);
    HoodieRecord newerRecord = mock(HoodieRecord.class);
    when(readerContext.getSchemaFromBufferRecord(T2)).thenReturn(olderRecorderSchema);
    when(readerContext.getSchemaFromBufferRecord(T1)).thenReturn(newerRecorderSchema);
    when(readerContext.constructHoodieRecord(T2)).thenReturn(olderRecord);
    when(readerContext.constructHoodieRecord(T1)).thenReturn(newerRecord);

    // Mock result
    when(recordMerger.merge(olderRecord, olderRecorderSchema, newerRecord, newerRecorderSchema, props))
        .thenReturn(Option.empty());

    EngineBasedMerger<TestRecord> merger = new EngineBasedMerger<>(readerContext, RecordMergeMode.CUSTOM, null, props);

    BufferedRecord<TestRecord> result = merger.merge(Option.of(T2), Option.of(T1), false);
    BufferedRecord<TestRecord> expected;
    if (mergingStrategy.equals(HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID)) {
      expected = new BufferedRecord<>(T1.getRecordKey(), T1.getOrderingValue(), T1.getRecord(), T1.getSchemaId(), true);
    } else {
      expected = new BufferedRecord<>(T2.getRecordKey(), T2.getOrderingValue(), T2.getRecord(), T2.getSchemaId(), true);
    }
    assertEquals(expected, result);
  }

  private void mockResultConversionToBufferedRecord(Integer schemaId, HoodieRecord rewrittenRecord, long orderingValue, String recordKey, TestRecord data, boolean isDelete) throws IOException {
    when(readerContext.encodeAvroSchema(readerSchema)).thenReturn(schemaId);
    when(rewrittenRecord.getOrderingValue(readerSchema, props)).thenReturn(orderingValue);
    when(rewrittenRecord.getKey()).thenReturn(new HoodieKey(recordKey, ""));
    when(rewrittenRecord.isDelete(readerSchema, props)).thenReturn(isDelete);
    when(rewrittenRecord.getData()).thenReturn(data);
  }

  private void mockMergeCallAndRewriteWithReaderSchema(HoodieRecordMerger recordMerger, HoodieRecord olderRecord, Schema olderRecorderSchema,
                                                       HoodieRecord newerRecord, Schema newerRecorderSchema, HoodieRecord rewrittenRecord)
      throws IOException {
    HoodieRecord mergedRecord = mock(HoodieRecord.class);
    Schema mergedSchema = mock(Schema.class);
    when(recordMerger.merge(olderRecord, olderRecorderSchema, newerRecord, newerRecorderSchema, props))
        .thenReturn(Option.of(Pair.of(mergedRecord, mergedSchema)));

    // Mock rewriting the record with the readerSchema
    when(mergedRecord.rewriteRecordWithNewSchema(mergedSchema, props, readerSchema)).thenReturn(rewrittenRecord);
  }

  private void mockRecordConversion(Schema olderRecorderSchema, Schema newerRecorderSchema, HoodieRecord olderRecord, HoodieRecord newerRecord) {
    when(readerContext.getSchemaFromBufferRecord(T1)).thenReturn(olderRecorderSchema);
    when(readerContext.getSchemaFromBufferRecord(T2)).thenReturn(newerRecorderSchema);
    when(readerContext.constructHoodieRecord(T1)).thenReturn(olderRecord);
    when(readerContext.constructHoodieRecord(T2)).thenReturn(newerRecord);
  }

  private void mockEmptyMergerAndSchema() {
    when(readerContext.getRecordMerger()).thenReturn(Option.empty());
    when(readerContext.getSchemaHandler().getRequiredSchema()).thenReturn(readerSchema);
  }

  private static class TestRecord {
    // placeholder class for ease of testing
  }
}
