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

import org.apache.hudi.common.avro.AvroRecordContext;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  @Test
  void testEventTimeMergeRejectsNullOrderingValues() {
    BufferedRecordMerger<String> merger = merger(RecordMergeMode.EVENT_TIME_ORDERING);
    BufferedRecord<String> missing = bufferedRecord("missing", null);
    BufferedRecord<String> present = bufferedRecord("present", 100L);
    assertFalse(missing.isCommitTimeOrderingDelete());
    assertThrows(NullPointerException.class, () -> merger.finalMerge(missing, present));
    assertThrows(NullPointerException.class, () -> merger.finalMerge(present, missing));
    assertThrows(NullPointerException.class, () -> merger.finalMerge(missing, missing));
    assertThrows(NullPointerException.class, () -> merger.deltaMerge(present, missing));
    assertThrows(NullPointerException.class, () -> merger.deltaMerge(missing, present));
    assertThrows(NullPointerException.class, () -> merger.deltaMerge(missing, missing));
  }

  @Test
  void testEventTimeMergeRejectsUnrelatedOrderingTypes() {
    BufferedRecordMerger<String> merger = merger(RecordMergeMode.EVENT_TIME_ORDERING);
    assertThrows(ClassCastException.class,
        () -> merger.finalMerge(bufferedRecord("old", "100"), bufferedRecord("new", 100L)));
    Comparable withNull = OrderingValues.create(new Comparable[] {1L, null});
    Comparable withValue = OrderingValues.create(new Comparable[] {1L, 2L});
    assertThrows(NullPointerException.class,
        () -> merger.finalMerge(bufferedRecord("old", withNull), bufferedRecord("new", withValue)));
  }

  @Test
  void testCommitTimeMergeAcceptsNullOrderingValues() throws IOException {
    BufferedRecordMerger<String> merger = merger(RecordMergeMode.COMMIT_TIME_ORDERING);
    BufferedRecord<String> older = bufferedRecord("old", null);
    BufferedRecord<String> newer = bufferedRecord("new", null);
    assertSame(newer, merger.finalMerge(older, newer));
    assertSame(newer, merger.deltaMerge(newer, older).get());
    assertSame(newer, merger.finalMerge(bufferedRecord("old", 100L), newer));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(ints = {0})
  void testEventTimeMergeRetainsOrderingAndDeleteSentinels(Integer deleteOrderingValue) throws IOException {
    BufferedRecordMerger<String> merger = merger(RecordMergeMode.EVENT_TIME_ORDERING);
    BufferedRecord<String> older = bufferedRecord("old", 200L);
    BufferedRecord<String> newer = bufferedRecord("new", 100L);
    assertSame(older, merger.finalMerge(older, newer));
    assertSame(older, merger.finalMerge(newer, older));
    assertSame(newer, merger.finalMerge(bufferedRecord("old", 100L), newer));

    BufferedRecord<String> delete = BufferedRecords.createDelete("key1", deleteOrderingValue);
    assertTrue(delete.isCommitTimeOrderingDelete());
    assertSame(delete, merger.finalMerge(older, delete));
    assertSame(newer, merger.finalMerge(delete, newer));
    assertSame(delete, merger.deltaMerge(delete, older).get());
    assertSame(newer, merger.deltaMerge(newer, delete).get());
    // Legacy integer sentinels still compare normally when both values have that type.
    BufferedRecord<String> legacy = bufferedRecord("legacy", 0);
    assertSame(legacy, merger.finalMerge(bufferedRecord("old", 0), legacy));
  }

  @ParameterizedTest
  @EnumSource(value = RecordMergeMode.class, names = {"COMMIT_TIME_ORDERING", "EVENT_TIME_ORDERING"})
  void testMissingOrderingFieldsUseNaturalOrder(RecordMergeMode mode) throws IOException {
    AvroRecordContext context = AvroRecordContext.getFieldAccessorInstance();
    BufferedRecord<String> older = bufferedRecord("old", context.getOrderingValue(null, null, Collections.emptyList()));
    BufferedRecord<String> newer = bufferedRecord("new", context.getOrderingValue(null, null, new String[0]));
    assertEquals(Integer.valueOf(0), older.getOrderingValue());
    assertEquals(Integer.valueOf(0), newer.getOrderingValue());
    BufferedRecordMerger<String> merger = merger(mode);
    assertSame(newer, merger.finalMerge(older, newer));
    assertSame(newer, merger.deltaMerge(newer, older).get());
  }

  @Test
  void testMaxOrderingValueRejectsNulls() {
    BufferedRecord<String> missing = bufferedRecord("missing", null);
    BufferedRecord<String> present = bufferedRecord("present", 100L);
    assertThrows(NullPointerException.class, () -> HoodieRecordMerger.maxOrderingValue(missing, missing));
    assertThrows(NullPointerException.class, () -> HoodieRecordMerger.maxOrderingValue(missing, present));
    assertThrows(NullPointerException.class, () -> HoodieRecordMerger.maxOrderingValue(present, missing));
    assertEquals(200L, HoodieRecordMerger.maxOrderingValue(present, bufferedRecord("new", 200L)));
    BufferedRecord<String> defaultOrdering = bufferedRecord("default", OrderingValues.getDefault());
    assertEquals(OrderingValues.getDefault(), HoodieRecordMerger.maxOrderingValue(defaultOrdering, defaultOrdering));
  }

  private static BufferedRecordMerger<String> merger(RecordMergeMode mode) {
    HoodieReaderContext<String> context = mock(HoodieReaderContext.class);
    when(context.getRecordContext()).thenReturn(mock(RecordContext.class));
    return BufferedRecordMergerFactory.create(context, mode, false, Option.empty(),
        HoodieSchema.create(HoodieSchemaType.STRING), Option.empty(), new TypedProperties(), Option.empty());
  }

  private static BufferedRecord<String> bufferedRecord(String value, Comparable orderingValue) {
    return new BufferedRecord<>("key1", orderingValue, value, 1, null);
  }
}
