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

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.BaseAvroPayload;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecord.SENTINEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class TestUpdateProcessor {
  private final HoodieReaderContext<IndexedRecord> readerContext = mock(HoodieReaderContext.class, RETURNS_DEEP_STUBS);
  private final RecordContext<IndexedRecord> recordContext = mock(RecordContext.class);
  private static final String KEY = "key";
  private static final Schema SCHEMA = SchemaBuilder.record("TestRecord")
      .fields()
      .name("key").type().stringType().noDefault()
      .name("value").type().stringType().noDefault()
      .endRecord();

  private static Stream<Arguments> handleEmitDeletes() {
    BufferedRecord<IndexedRecord> previous = getRecord("value1", null);
    BufferedRecord<IndexedRecord> merged = getRecord("value2", null);
    BufferedRecord<IndexedRecord> mergedWithUpdateBefore = getRecord("value2", HoodieOperation.UPDATE_BEFORE);
    BufferedRecord<IndexedRecord> mergedWithEmpty = new BufferedRecord<>(KEY, 1, null, 0, null);

    BufferedRecord<IndexedRecord> expected = getRecord("value2", HoodieOperation.DELETE);
    BufferedRecord<IndexedRecord> expectedWithUpdateBefore = getRecord("value2", HoodieOperation.UPDATE_BEFORE);

    return Stream.of(
        Arguments.of(previous, merged, expected, true),
        Arguments.of(previous, merged, expected, false),
        Arguments.of(previous, mergedWithUpdateBefore, expectedWithUpdateBefore, true),
        Arguments.of(previous, mergedWithUpdateBefore, expectedWithUpdateBefore, false),
        Arguments.of(previous, mergedWithEmpty, null, true),
        Arguments.of(previous, mergedWithEmpty, null, false));
  }

  @ParameterizedTest
  @MethodSource("handleEmitDeletes")
  void testHandleEmitDeletes(BufferedRecord<IndexedRecord> previous, BufferedRecord<IndexedRecord> merged, BufferedRecord<IndexedRecord> expected, boolean usePayload) {
    if (merged.getRecord() == null) {
      when(readerContext.getRecordContext()).thenReturn(recordContext);
    }
    HoodieReadStats readStats = new HoodieReadStats();
    BaseFileUpdateCallback<IndexedRecord> updateCallback = mock(BaseFileUpdateCallback.class);
    UpdateProcessor.StandardUpdateProcessor<IndexedRecord> delegate;
    if (usePayload) {
      delegate = new UpdateProcessor.PayloadUpdateProcessor<>(readStats, readerContext, true, new Properties(), "org.apache.hudi.common.testutils.DummyPayload");
    } else {
      delegate = new UpdateProcessor.StandardUpdateProcessor<>(readStats, readerContext, true);
    }
    UpdateProcessor<IndexedRecord> updateProcessor = new UpdateProcessor.CallbackProcessor<>(updateCallback, delegate);
    BufferedRecord<IndexedRecord> result = updateProcessor.processUpdate(KEY, previous, merged, true);
    assertEquals(expected, result);
    verifyReadStats(readStats, 0, 0, 1);
    if (merged.getRecord() == null) {
      verify(recordContext).getDeleteRow(KEY);
    }
    verify(updateCallback).onDelete(KEY, previous, merged.getHoodieOperation());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHandleDelete(boolean usePayload) {
    BufferedRecord<IndexedRecord> previous = getRecord("value1", null);
    BufferedRecord<IndexedRecord> merged = getRecord("value2", null);
    HoodieReadStats readStats = new HoodieReadStats();
    BaseFileUpdateCallback<IndexedRecord> updateCallback = mock(BaseFileUpdateCallback.class);
    UpdateProcessor.StandardUpdateProcessor<IndexedRecord> delegate;
    if (usePayload) {
      delegate = new UpdateProcessor.PayloadUpdateProcessor<>(readStats, readerContext, false, new Properties(), "org.apache.hudi.common.testutils.DummyPayload");
    } else {
      delegate = new UpdateProcessor.StandardUpdateProcessor<>(readStats, readerContext, false);
    }
    UpdateProcessor<IndexedRecord> updateProcessor = new UpdateProcessor.CallbackProcessor<>(updateCallback, delegate);
    BufferedRecord<IndexedRecord> result = updateProcessor.processUpdate(KEY, previous, merged, true);
    assertNull(result);
    verifyReadStats(readStats, 0, 0, 1);
    verify(updateCallback).onDelete(KEY, previous, merged.getHoodieOperation());
  }

  @Test
  void testHandleInsert() {
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(recordContext.seal(any())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
    BufferedRecord<IndexedRecord> previous = null;
    BufferedRecord<IndexedRecord> merged = getRecord("value2", null);
    BufferedRecord<IndexedRecord> expected = getRecord("value2", HoodieOperation.INSERT);
    HoodieReadStats readStats = new HoodieReadStats();
    BaseFileUpdateCallback<IndexedRecord> updateCallback = mock(BaseFileUpdateCallback.class);
    UpdateProcessor<IndexedRecord> updateProcessor = new UpdateProcessor.CallbackProcessor<>(updateCallback, new UpdateProcessor.StandardUpdateProcessor<>(readStats, readerContext, false));
    BufferedRecord<IndexedRecord> result = updateProcessor.processUpdate(KEY, previous, merged, false);
    assertEquals(expected, result);
    verifyReadStats(readStats, 1, 0, 0);
    verify(updateCallback).onInsert(KEY, merged);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHandleInsertWithPayload(boolean shouldIgnore) {
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(recordContext.seal(any())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
    BufferedRecord<IndexedRecord> previous = null;
    BufferedRecord<IndexedRecord> merged = getRecord("value2", null);
    BufferedRecord<IndexedRecord> expected = getRecordWithSerializableIndexedRecord("value2", HoodieOperation.INSERT);
    HoodieReadStats readStats = new HoodieReadStats();
    BaseFileUpdateCallback<IndexedRecord> updateCallback = mock(BaseFileUpdateCallback.class);
    UpdateProcessor<IndexedRecord> updateProcessor = new UpdateProcessor.CallbackProcessor<>(updateCallback, new UpdateProcessor.PayloadUpdateProcessor<>(readStats, readerContext, false,
        new Properties(), DummyPayload.class.getName()));

    // mock record creation
    when(recordContext.decodeAvroSchema(merged.getSchemaId())).thenReturn(SCHEMA);
    when(recordContext.convertToAvroRecord(merged.getRecord(), SCHEMA)).thenReturn((GenericRecord) merged.getRecord());
    if (shouldIgnore) {
      when(recordContext.convertToAvroRecord(merged.getRecord(), SCHEMA)).thenReturn(SENTINEL);
    } else {
      when(recordContext.convertToAvroRecord(merged.getRecord(), SCHEMA)).thenReturn((GenericRecord) merged.getRecord());
      when(readerContext.getSchemaHandler().getRequestedSchema()).thenReturn(SCHEMA);
      when(recordContext.convertAvroRecord(any())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
    }

    BufferedRecord<IndexedRecord> result = updateProcessor.processUpdate(KEY, previous, merged, false);
    if (shouldIgnore) {
      assertNull(result);
      verifyReadStats(readStats, 0, 0, 0);
      verifyNoInteractions(updateCallback);
    } else {
      assertEquals(expected, result);
      verifyReadStats(readStats, 1, 0, 0);
      verify(updateCallback).onInsert(KEY, merged);
    }
  }

  private static BufferedRecord<IndexedRecord> getRecordWithSerializableIndexedRecord(String value, HoodieOperation operation) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("key", KEY);
    record.put("value", value);
    return new BufferedRecord<>(KEY, 1, new HoodieAvroIndexedRecord(record).getData(), 0, operation);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHandleNoUpdate(boolean usePayload) {
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(recordContext.seal(any())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
    BufferedRecord<IndexedRecord> previous = getRecord("value2", null);
    BufferedRecord<IndexedRecord> merged = previous;
    HoodieReadStats readStats = new HoodieReadStats();
    BaseFileUpdateCallback<IndexedRecord> updateCallback = mock(BaseFileUpdateCallback.class);
    UpdateProcessor.StandardUpdateProcessor<IndexedRecord> delegate;
    if (usePayload) {
      delegate = new UpdateProcessor.PayloadUpdateProcessor<>(readStats, readerContext, false, new Properties(), "org.apache.hudi.common.testutils.DummyPayload");
    } else {
      delegate = new UpdateProcessor.StandardUpdateProcessor<>(readStats, readerContext, false);
    }
    UpdateProcessor<IndexedRecord> updateProcessor = new UpdateProcessor.CallbackProcessor<>(updateCallback, delegate);
    BufferedRecord<IndexedRecord> result = updateProcessor.processUpdate(KEY, previous, merged, false);
    assertEquals(merged, result);
    verifyReadStats(readStats, 0, 0, 0);
    verifyNoInteractions(updateCallback);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHandleUpdate(boolean usePayload) {
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(recordContext.seal(any())).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));
    BufferedRecord<IndexedRecord> previous = getRecord("value1", null);
    BufferedRecord<IndexedRecord> merged = getRecord("value2", null);
    BufferedRecord<IndexedRecord> expected = getRecord("value2", HoodieOperation.UPDATE_AFTER);
    HoodieReadStats readStats = new HoodieReadStats();
    BaseFileUpdateCallback<IndexedRecord> updateCallback = mock(BaseFileUpdateCallback.class);
    UpdateProcessor.StandardUpdateProcessor<IndexedRecord> delegate;
    if (usePayload) {
      delegate = new UpdateProcessor.PayloadUpdateProcessor<>(readStats, readerContext, false, new Properties(), "org.apache.hudi.common.testutils.DummyPayload");
    } else {
      delegate = new UpdateProcessor.StandardUpdateProcessor<>(readStats, readerContext, false);
    }
    UpdateProcessor<IndexedRecord> updateProcessor = new UpdateProcessor.CallbackProcessor<>(updateCallback, delegate);
    BufferedRecord<IndexedRecord> result = updateProcessor.processUpdate(KEY, previous, merged, false);
    assertEquals(expected, result);
    verifyReadStats(readStats, 0, 1, 0);
    verify(updateCallback).onUpdate(KEY, previous, merged);
  }

  private void verifyReadStats(HoodieReadStats readStats, int numInserts, int numUpdates, int numDeletes) {
    assertEquals(numInserts, readStats.getNumInserts());
    assertEquals(numUpdates, readStats.getNumUpdates());
    assertEquals(numDeletes, readStats.getNumDeletes());
  }

  private static BufferedRecord<IndexedRecord> getRecord(String value, HoodieOperation operation) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("key", KEY);
    record.put("value", value);
    return new BufferedRecord<>(KEY, 1, record, 0, operation);
  }

  public static class DummyPayload extends BaseAvroPayload implements HoodieRecordPayload<DummyPayload> {
    private final boolean isSentinel;

    public DummyPayload(GenericRecord record, Comparable orderingVal) {
      super(record == SENTINEL ? null : record, orderingVal);
      this.isSentinel = record == SENTINEL;
    }

    @Override
    public boolean canProduceSentinel() {
      return true;
    }

    @Override
    public DummyPayload preCombine(DummyPayload oldValue) {
      return null;
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
      return null;
    }

    @Override
    public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
      if (isSentinel) {
        return Option.of(SENTINEL);
      }
      return getRecord(schema);
    }
  }
}
