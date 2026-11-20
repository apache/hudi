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

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.time.LocalDate;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.OrderingValues.getDefault;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBufferedRecordMergerFactory {
  private static final String KEY = "key0";

  @Test
  void testSelectsMergerForEveryConfiguredMode() {
    HoodieReaderContext<String> context = mock(HoodieReaderContext.class);
    when(context.getRecordContext()).thenReturn(mock(RecordContext.class));
    HoodieSchema schema = HoodieSchema.create(HoodieSchemaType.STRING);
    TypedProperties props = new TypedProperties();
    Option<HoodieRecordMerger> recordMerger = Option.of(mock(HoodieRecordMerger.class));

    assertMerger("CommitTimeRecordMerger", context, RecordMergeMode.COMMIT_TIME_ORDERING, false,
        recordMerger, schema, Option.empty(), props, Option.empty());
    assertMerger("CommitTimePartialRecordMerger", context, RecordMergeMode.COMMIT_TIME_ORDERING, false,
        recordMerger, schema, Option.empty(), props, Option.of(PartialUpdateMode.IGNORE_DEFAULTS));
    assertMerger("EventTimeRecordMerger", context, RecordMergeMode.EVENT_TIME_ORDERING, false,
        recordMerger, schema, Option.empty(), props, Option.empty());
    assertMerger("EventTimePartialRecordMerger", context, RecordMergeMode.EVENT_TIME_ORDERING, false,
        recordMerger, schema, Option.empty(), props, Option.of(PartialUpdateMode.FILL_UNAVAILABLE));
    assertMerger("PartialUpdateBufferedRecordMerger", context, RecordMergeMode.EVENT_TIME_ORDERING, true,
        recordMerger, schema, Option.empty(), props, Option.empty());
    assertMerger("CustomRecordMerger", context, RecordMergeMode.CUSTOM, false,
        recordMerger, schema, Option.empty(), props, Option.empty());
    assertMerger("CustomPayloadRecordMerger", context, RecordMergeMode.CUSTOM, false,
        recordMerger, schema, Option.of(Pair.of("table.Payload", "incoming.Payload")), props, Option.empty());
    assertMerger("ExpressionPayloadRecordMerger", context, RecordMergeMode.CUSTOM, false,
        recordMerger, schema, Option.of(Pair.of("table.Payload", "org.apache.spark.sql.hudi.command.payload.ExpressionPayload")), props, Option.empty());

    assertEquals("CustomPayloadRecordMerger", BufferedRecordMergerFactory.create(
        context, RecordMergeMode.CUSTOM, false, recordMerger, Option.of("payload.Class"), schema, props, Option.empty())
        .getClass().getSimpleName());
  }

  @Test
  void testLegacyDeleteMergePreservesSentinelsBeforeConversion() throws IOException {
    RecordContext<String> recordContext = mock(RecordContext.class, CALLS_REAL_METHODS);
    when(recordContext.convertOrderingValueToEngineType(any())).thenAnswer(
        invocation -> LocalDate.ofEpochDay(((Number) invocation.getArgument(0)).longValue()));
    HoodieReaderContext<String> readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    BufferedRecordMerger<String> merger = BufferedRecordMergerFactory.create(
        readerContext, RecordMergeMode.EVENT_TIME_ORDERING, false, Option.empty(),
        HoodieSchema.create(HoodieSchemaType.STRING), Option.empty(), new TypedProperties(), Option.empty());
    BufferedRecord<String> existing = new BufferedRecord<>("key", LocalDate.ofEpochDay(10), "record", 0, null);

    for (Comparable orderingValue : new Comparable[] {null, 0}) {
      DeleteRecord delete = DeleteRecord.create("key", "", orderingValue);
      assertEquals(Option.of(delete), merger.deltaMerge(delete, existing));
    }
    assertEquals(Option.empty(), merger.deltaMerge(DeleteRecord.create("key", "", 9), existing));
  }

  static Stream<Arguments> storedNullOrderingValues() {
    return Stream.of(Arguments.of((Comparable) null), Arguments.of(getDefault()));
  }

  @ParameterizedTest
  @MethodSource("storedNullOrderingValues")
  void testEventTimeMergerLetsIncomingWinOverStoredNull(Comparable storedOrderingValue) throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger(Option.empty());
    BufferedRecord<String> stored = record(storedOrderingValue, "stored");
    BufferedRecord<String> incoming = record(5L, "incoming");

    assertSame(incoming, merger.finalMerge(stored, incoming));
    assertSame(incoming, merger.deltaMerge(incoming, stored).get());
  }

  @ParameterizedTest
  @MethodSource("storedNullOrderingValues")
  void testEventTimeMergerLetsIncomingDeleteWinOverStoredNull(Comparable storedOrderingValue) throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger(Option.empty());
    BufferedRecord<String> incomingDelete = new BufferedRecord<>(KEY, 5L, null, 0, HoodieOperation.DELETE);

    assertSame(incomingDelete, merger.finalMerge(record(storedOrderingValue, "stored"), incomingDelete));
  }

  @Test
  void testEventTimeMergerKeepsStoredRecordWithHigherOrderingValue() throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger(Option.empty());
    BufferedRecord<String> stored = record(9L, "stored");
    BufferedRecord<String> incoming = record(5L, "incoming");

    assertSame(stored, merger.finalMerge(stored, incoming));
    assertTrue(merger.deltaMerge(incoming, stored).isEmpty());
  }

  @Test
  void testEventTimeMergerRejectsMismatchedOrderingValues() {
    BufferedRecordMerger<String> merger = eventTimeMerger(Option.empty());

    assertThrows(IllegalArgumentException.class, () -> merger.finalMerge(record(9, "stored"), record(5L, "incoming")));
  }

  @ParameterizedTest
  @MethodSource("storedNullOrderingValues")
  void testEventTimePartialMergerLetsIncomingWinOverStoredNull(Comparable storedOrderingValue) throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger(Option.of(PartialUpdateMode.IGNORE_DEFAULTS));
    BufferedRecord<String> incomingDelete = new BufferedRecord<>(KEY, 5L, null, 0, HoodieOperation.DELETE);

    assertSame(incomingDelete, merger.finalMerge(record(storedOrderingValue, "stored"), incomingDelete));
  }

  @ParameterizedTest
  @MethodSource("storedNullOrderingValues")
  void testDeltaMergeDeleteRecordWinsOverStoredNull(Comparable storedOrderingValue) throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger(Option.empty());
    DeleteRecord deleteRecord = DeleteRecord.create(new HoodieKey(KEY, "partition0"), 5L);

    assertSame(deleteRecord, merger.deltaMerge(deleteRecord, record(storedOrderingValue, "stored")).get());
  }

  /**
   * A stored delete with a null ordering value is not a commit-time-ordering delete, so it reaches the class check.
   */
  @Test
  void testDeltaMergeDeleteRecordWinsOverStoredDeleteWithNullOrderingValue() throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger(Option.empty());
    BufferedRecord<String> storedDelete = new BufferedRecord<>(KEY, null, null, 0, HoodieOperation.DELETE);
    assertFalse(storedDelete.isCommitTimeOrderingDelete());
    DeleteRecord deleteRecord = DeleteRecord.create(new HoodieKey(KEY, "partition0"), 5L);

    assertSame(deleteRecord, merger.deltaMerge(deleteRecord, storedDelete).get());
  }

  @Test
  void testDeltaMergeDeleteRecordWinsOnMismatchedOrderingValue() throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger(Option.empty());
    DeleteRecord deleteRecord = DeleteRecord.create(new HoodieKey(KEY, "partition0"), 5L);

    assertSame(deleteRecord, merger.deltaMerge(deleteRecord, record(9, "stored")).get());
  }

  @Test
  void testDeltaMergeDeleteRecordKeepsStoredRecordWithHigherOrderingValue() throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger(Option.empty());
    DeleteRecord deleteRecord = DeleteRecord.create(new HoodieKey(KEY, "partition0"), 5L);

    assertTrue(merger.deltaMerge(deleteRecord, record(9L, "stored")).isEmpty());
  }

  private static BufferedRecordMerger<String> eventTimeMerger(Option<PartialUpdateMode> partialUpdateMode) {
    RecordContext<String> recordContext = mock(RecordContext.class, CALLS_REAL_METHODS);
    when(recordContext.convertOrderingValueToEngineType(any())).thenAnswer(invocation -> invocation.getArgument(0));
    HoodieReaderContext<String> readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getRecordContext()).thenReturn(recordContext);
    return BufferedRecordMergerFactory.create(readerContext, RecordMergeMode.EVENT_TIME_ORDERING, false, Option.empty(),
        HoodieSchema.create(HoodieSchemaType.STRING), Option.empty(), new TypedProperties(), partialUpdateMode);
  }

  private static BufferedRecord<String> record(Comparable orderingValue, String value) {
    return new BufferedRecord<>(KEY, orderingValue, value, 0, null);
  }

  private static void assertMerger(String expected,
                                   HoodieReaderContext<String> context,
                                   RecordMergeMode mode,
                                   boolean partialMerging,
                                   Option<HoodieRecordMerger> recordMerger,
                                   HoodieSchema schema,
                                   Option<Pair<String, String>> payloadClasses,
                                   TypedProperties props,
                                   Option<PartialUpdateMode> partialUpdateMode) {
    assertEquals(expected, BufferedRecordMergerFactory.create(
        context, mode, partialMerging, recordMerger, schema, payloadClasses, props, partialUpdateMode)
        .getClass().getSimpleName());
  }
}
