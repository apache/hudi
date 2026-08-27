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
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBufferedRecordMergerFactory {

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

  /**
   * A null ordering field value is represented as {@link OrderingValues#getDefault()}, an
   * {@code int}, while its counterpart in the same file group can hold the column's real value of
   * another type, e.g. a {@code Long}. Comparing the two through {@code Comparable#compareTo} threw
   * {@code ClassCastException}, which the write path surfaces as {@code HoodieUpsertException}.
   */
  @Test
  void testMergeToleratesOrderingValuesOfDifferentClasses() throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger();

    BufferedRecord<String> olderWithDefault = bufferedRecord("base", OrderingValues.getDefault());
    BufferedRecord<String> newerWithLong = bufferedRecord("log", 1000L);
    assertSame(newerWithLong, merger.finalMerge(olderWithDefault, newerWithLong));

    BufferedRecord<String> olderWithLong = bufferedRecord("base", 1000L);
    BufferedRecord<String> newerWithDefault = bufferedRecord("log", OrderingValues.getDefault());
    assertSame(newerWithDefault, merger.finalMerge(olderWithLong, newerWithDefault));

    Option<BufferedRecord<String>> delta = merger.deltaMerge(newerWithLong, olderWithDefault);
    assertTrue(delta.isPresent());
    assertSame(newerWithLong, delta.get());
  }

  /**
   * Event-time ordering is unchanged for ordering values of the same class.
   */
  @Test
  void testMergeRetainsEventTimeOrderingForSameClassOrderingValues() throws IOException {
    BufferedRecordMerger<String> merger = eventTimeMerger();

    BufferedRecord<String> newerWins = bufferedRecord("log", 2000L);
    assertSame(newerWins, merger.finalMerge(bufferedRecord("base", 1000L), newerWins));

    BufferedRecord<String> olderWins = bufferedRecord("base", 2000L);
    assertSame(olderWins, merger.finalMerge(olderWins, bufferedRecord("log", 1000L)));

    BufferedRecord<String> newerOnTie = bufferedRecord("log", 1000L);
    assertSame(newerOnTie, merger.finalMerge(bufferedRecord("base", 1000L), newerOnTie));

    BufferedRecord<String> newerWhenBothDefault = bufferedRecord("log", OrderingValues.getDefault());
    assertSame(newerWhenBothDefault,
        merger.finalMerge(bufferedRecord("base", OrderingValues.getDefault()), newerWhenBothDefault));
  }

  private static BufferedRecordMerger<String> eventTimeMerger() {
    HoodieReaderContext<String> context = mock(HoodieReaderContext.class);
    when(context.getRecordContext()).thenReturn(mock(RecordContext.class));
    return BufferedRecordMergerFactory.create(
        context, RecordMergeMode.EVENT_TIME_ORDERING, false, Option.of(mock(HoodieRecordMerger.class)),
        HoodieSchema.create(HoodieSchemaType.STRING), Option.empty(), new TypedProperties(), Option.empty());
  }

  private static BufferedRecord<String> bufferedRecord(String value, Comparable orderingValue) {
    return new BufferedRecord<>("key1", orderingValue, value, 1, null);
  }
}
