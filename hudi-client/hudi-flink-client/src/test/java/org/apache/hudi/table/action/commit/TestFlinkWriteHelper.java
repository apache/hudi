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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.PartialUpdateMode;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecordMergerFactory;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.table.format.FlinkRecordContext;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestFlinkWriteHelper {

  private static final String SCHEMA = "{\"type\":\"record\",\"name\":\"testrec\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
  private static final String ROW_SCHEMA = "{\"type\":\"record\",\"name\":\"testrow\",\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"},"
      + "{\"name\":\"value\",\"type\":[\"null\",\"string\"],\"default\":null}]}";
  private static final String[] ORDERING_FIELDS = {"ts"};

  @SuppressWarnings("unchecked")
  private final FlinkWriteHelper<RowData, Object> rowWriteHelper = FlinkWriteHelper.newInstance();

  @Test
  void testSortedDeduplicationEmptyInput() {
    Iterator<HoodieRecord<RowData>> result = rowWriteHelper.deduplicateSortedRecords(
        Collections.emptyIterator(), true, ROW_SCHEMA, new TypedProperties(), null, null, ORDERING_FIELDS);
    assertFalse(result.hasNext());
    assertFalse(result.hasNext());
    assertThrows(NoSuchElementException.class, result::next);
  }

  @Test
  void testSortedDeduplicationIsLazyAndPreservesUniqueRecords() {
    List<HoodieRecord<RowData>> records = Arrays.asList(
        row("a", 1, "first", HoodieOperation.INSERT),
        row("b", 2, "second", HoodieOperation.INSERT),
        row("c", 3, "last", HoodieOperation.INSERT));
    AtomicInteger consumed = new AtomicInteger();
    Iterator<HoodieRecord<RowData>> input = new MappingIterator<>(records.iterator(), record -> {
      consumed.incrementAndGet();
      return record;
    });
    Iterator<HoodieRecord<RowData>> result = rowWriteHelper.deduplicateSortedRecords(
        input, true, ROW_SCHEMA, new TypedProperties(), null, null, ORDERING_FIELDS);
    assertEquals(0, consumed.get());
    assertTrue(result.hasNext());
    assertTrue(result.hasNext());
    assertEquals(0, consumed.get());
    assertSame(records.get(0), result.next());
    assertEquals(2, consumed.get(), "Only the current group and one lookahead record may be consumed");
    assertSame(records.get(1), result.next());
    assertSame(records.get(2), result.next());
    assertFalse(result.hasNext());
    assertThrows(NoSuchElementException.class, result::next);
  }

  @ParameterizedTest
  @CsvSource({"EVENT_TIME_ORDERING,false", "COMMIT_TIME_ORDERING,false",
      "EVENT_TIME_ORDERING,true", "COMMIT_TIME_ORDERING,true"})
  void testSortedDeduplicationMatchesGroupedReduction(RecordMergeMode mode, boolean partialUpdate) {
    HoodieReaderContext<RowData> readerContext = readerContext();
    BufferedRecordMerger<RowData> merger = BufferedRecordMergerFactory.create(
        readerContext, mode, false, Option.empty(), Option.empty(), HoodieSchema.parse(ROW_SCHEMA),
        new TypedProperties(), partialUpdate ? Option.of(PartialUpdateMode.IGNORE_DEFAULTS) : Option.empty());
    List<HoodieRecord<RowData>> records = Arrays.asList(
        row("a", 3, "first", HoodieOperation.INSERT),
        row("a", 3, "tie", HoodieOperation.UPDATE_AFTER),
        row("a", 2, "older", HoodieOperation.UPDATE_AFTER),
        row("b", 1, "before-delete", HoodieOperation.INSERT),
        row("b", 2, null, HoodieOperation.DELETE),
        row("b", 3, "reinsert", HoodieOperation.INSERT),
        row("c", 1, "deleted", HoodieOperation.INSERT),
        row("c", 2, null, HoodieOperation.DELETE),
        row("d", 1, "retained", HoodieOperation.INSERT),
        row("d", 2, null, HoodieOperation.UPDATE_AFTER));
    List<HoodieRecord<RowData>> actual = assertMatchesGroupedReduction(records, true, merger, readerContext);
    assertEquals(Arrays.asList("a", "b", "c", "d"),
        actual.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));
    assertEquals(mode == RecordMergeMode.EVENT_TIME_ORDERING ? "tie" : "older", actual.get(0).getData().getString(2).toString());
    assertEquals("reinsert", actual.get(1).getData().getString(2).toString());
    assertTrue(actual.get(2).isDelete(
        DeleteContext.fromRecordSchema(new TypedProperties(), HoodieSchema.parse(ROW_SCHEMA)), new TypedProperties()));
    if (partialUpdate) {
      assertEquals("retained", actual.get(3).getData().getString(2).toString());
    } else {
      assertTrue(actual.get(3).getData().isNullAt(2));
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void testSortedDeduplicationPreservesCustomMergeOrderAndEmptyResult() throws Exception {
    BufferedRecordMerger<RowData> merger = mock(BufferedRecordMerger.class);
    when(merger.deltaMerge(any(BufferedRecord.class), any(BufferedRecord.class))).thenAnswer(invocation -> {
      BufferedRecord<RowData> next = invocation.getArgument(0);
      BufferedRecord<RowData> previous = invocation.getArgument(1);
      String value = next.getRecord().getString(2).toString();
      if (value.equals("skip")) {
        return Option.empty();
      }
      // Non-associative merging detects changes in reduction order and argument direction.
      RowData merged = GenericRowData.of(StringData.fromString(next.getRecordKey()), next.getOrderingValue(),
          StringData.fromString("(" + previous.getRecord().getString(2) + "," + value + ")"));
      return Option.of(new BufferedRecord<>(next.getRecordKey(), next.getOrderingValue(), merged,
          next.getSchemaId(), next.getHoodieOperation()));
    });
    List<HoodieRecord<RowData>> records = Arrays.asList(
        row("a", 1, "1", HoodieOperation.INSERT),
        row("a", 2, "2", HoodieOperation.UPDATE_AFTER),
        row("a", 3, "skip", HoodieOperation.UPDATE_AFTER),
        row("a", 4, "3", HoodieOperation.UPDATE_AFTER));
    List<HoodieRecord<RowData>> actual = assertMatchesGroupedReduction(records, true, merger, readerContext());
    assertEquals(1, actual.size());
    assertEquals("((1,2),3)", actual.get(0).getData().getString(2).toString());
  }

  @Test
  void testUnsortedDeduplicationMergesNonAdjacentKeysAndPreservesKeyOrder() {
    HoodieReaderContext<RowData> readerContext = readerContext();
    BufferedRecordMerger<RowData> merger = BufferedRecordMergerFactory.create(
        readerContext, RecordMergeMode.EVENT_TIME_ORDERING, false, Option.empty(), Option.empty(),
        HoodieSchema.parse(ROW_SCHEMA), new TypedProperties(), Option.empty());
    List<HoodieRecord<RowData>> records = Arrays.asList(
        row("b", 1, "old-b", HoodieOperation.INSERT),
        row("a", 3, "new-a", HoodieOperation.INSERT),
        row("b", 2, "new-b", HoodieOperation.UPDATE_AFTER),
        row("c", 1, "only-c", HoodieOperation.INSERT),
        row("a", 2, "old-a", HoodieOperation.UPDATE_AFTER));
    List<HoodieRecord<RowData>> actual = assertMatchesGroupedReduction(records, false, merger, readerContext);
    assertEquals(Arrays.asList("b", "a", "c"),
        actual.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));
    assertEquals("new-b", actual.get(0).getData().getString(2).toString());
    assertEquals("new-a", actual.get(1).getData().getString(2).toString());
  }

  private List<HoodieRecord<RowData>> assertMatchesGroupedReduction(
      List<HoodieRecord<RowData>> records, boolean isSortedByRecordKey,
      BufferedRecordMerger<RowData> merger, HoodieReaderContext<RowData> readerContext) {
    TypedProperties props = new TypedProperties();
    List<HoodieRecord<RowData>> expected = CollectionUtils.toList(rowWriteHelper.deduplicateRecords(
        records.iterator(), null, -1, ROW_SCHEMA, props, merger, readerContext, ORDERING_FIELDS));
    List<HoodieRecord<RowData>> actual = CollectionUtils.toList(rowWriteHelper.deduplicateSortedRecords(
        records.iterator(), isSortedByRecordKey, ROW_SCHEMA, props, merger, readerContext, ORDERING_FIELDS));
    assertEquals(expected.size(), actual.size());
    HoodieSchema schema = HoodieSchema.parse(ROW_SCHEMA);
    DeleteContext deleteContext = DeleteContext.fromRecordSchema(props, schema);
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i).getKey(), actual.get(i).getKey());
      assertEquals(expected.get(i).getOperation(), actual.get(i).getOperation());
      assertEquals(expected.get(i).getOrderingValue(schema, props, ORDERING_FIELDS), actual.get(i).getOrderingValue(schema, props, ORDERING_FIELDS));
      assertEquals(expected.get(i).isDelete(deleteContext, props), actual.get(i).isDelete(deleteContext, props));
      assertEquals(expected.get(i).getData(), actual.get(i).getData());
    }
    return actual;
  }

  @SuppressWarnings("unchecked")
  private HoodieReaderContext<RowData> readerContext() {
    HoodieReaderContext<RowData> readerContext = mock(HoodieReaderContext.class);
    when(readerContext.getRecordContext()).thenReturn(FlinkRecordContext.getDeleteCheckingInstance());
    return readerContext;
  }

  private HoodieRecord<RowData> row(String key, long orderingValue, String value, HoodieOperation operation) {
    GenericRowData data = GenericRowData.of(StringData.fromString(key), orderingValue,
        value == null ? null : StringData.fromString(value));
    return new HoodieFlinkRecord(new HoodieKey(key, "partition"), operation, data);
  }

  @Test
  void testDeduplicateRecordsPreservesInputKeyOrder() {
    List<HoodieRecord<HoodieAvroPayload>> records = Arrays.asList(record("b"), record("a"), record("c"));
    @SuppressWarnings("unchecked")
    FlinkWriteHelper<HoodieAvroPayload, Object> writeHelper = FlinkWriteHelper.newInstance();

    List<String> deduplicatedKeys = CollectionUtils.toStream(
        writeHelper.deduplicateRecords(
            records.iterator(),
            null,
            -1,
            SCHEMA,
            new TypedProperties(),
            null,
            null,
            new String[0]))
        .map(HoodieRecord::getRecordKey)
        .collect(Collectors.toList());

    assertEquals(Arrays.asList("b", "a", "c"), deduplicatedKeys);
  }

  private static HoodieRecord<HoodieAvroPayload> record(String recordKey) {
    return new HoodieAvroRecord<>(new HoodieKey(recordKey, "partition"), null);
  }
}
