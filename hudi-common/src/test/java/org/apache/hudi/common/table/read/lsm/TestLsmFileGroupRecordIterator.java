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

package org.apache.hudi.common.table.read.lsm;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemas;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestLsmFileGroupRecordIterator {

  @Test
  void testDeleteLogSchemaUsesRecordKeyAndOrderingFields() {
    HoodieSchema deleteLogSchema = HoodieSchemas.createDeleteLogSchema(tableSchema(), Arrays.asList("ts"));

    assertEquals(Arrays.asList(HoodieRecord.RECORD_KEY_METADATA_FIELD, "ts"), deleteLogSchema.getFields().stream()
        .map(HoodieSchemaField::name)
        .collect(Collectors.toList()));
    assertEquals(HoodieSchemaType.STRING, deleteLogSchema.getField(HoodieRecord.RECORD_KEY_METADATA_FIELD).get().schema().getType());
    HoodieSchemaField orderingField = deleteLogSchema.getField("ts").get();
    assertTrue(orderingField.schema().isNullable());
    assertEquals(HoodieSchemaType.LONG, orderingField.getNonNullSchema().getType());
    assertEquals(HoodieSchema.NULL_VALUE, orderingField.defaultVal().get());
  }

  @Test
  void testLoserTreeMergesByRecordKeyThenMergeOrder() {
    List<LsmFileGroupRecordIterator.SortedRunReader<String>> readers = Arrays.asList(
        sortedRunReader(0, record("key1", "base-key1"), record("key3", "base-key3")),
        sortedRunReader(1, record("key1", "log1-key1"), record("key2", "log1-key2")),
        sortedRunReader(2, BufferedRecords.createDelete("key1", 3), record("key3", "log2-key3")));

    LsmFileGroupRecordIterator.LoserTree<String> loserTree = new LsmFileGroupRecordIterator.LoserTree<>(readers);

    assertEquals(Arrays.asList(
        "key1:base-key1",
        "key1:log1-key1",
        "key1:DELETE",
        "key2:log1-key2",
        "key3:base-key3",
        "key3:log2-key3"), drain(loserTree));
  }

  @Test
  void testNativeDeleteRecordPreservesOrderingValue() {
    HoodieReaderContext<Map<String, Object>> readerContext = mock(HoodieReaderContext.class);
    RecordContext<Map<String, Object>> recordContext = mock(RecordContext.class);
    Map<String, Object> record = Collections.emptyMap();
    List<String> orderingFields = Collections.singletonList("ts");
    HoodieSchema deleteLogSchema = HoodieSchemas.createDeleteLogSchema(tableSchema(), orderingFields);

    when(readerContext.getRecordContext()).thenReturn(recordContext);
    when(recordContext.getValue(record, deleteLogSchema, HoodieRecord.RECORD_KEY_METADATA_FIELD)).thenReturn("key1");
    when(recordContext.getOrderingValue(eq(record), eq(deleteLogSchema), eq(orderingFields))).thenReturn(42L);

    BufferedRecord<Map<String, Object>> deleteRecord =
        LsmFileGroupRecordIterator.createNativeDeleteRecord(readerContext, record, deleteLogSchema, orderingFields);

    assertEquals("key1", deleteRecord.getRecordKey());
    assertEquals(42L, deleteRecord.getOrderingValue());
    assertTrue(deleteRecord.isDelete());
  }

  @Test
  void testSelectDirectLogReadersPrioritizesDeletesThenSmallFiles() {
    List<LsmFileGroupRecordIterator.LogReaderSpec> logReaderSpecs = Arrays.asList(
        new LsmFileGroupRecordIterator.LogReaderSpec(1, logFile("file1_1-0-1_001_1.log.parquet", 10)),
        new LsmFileGroupRecordIterator.LogReaderSpec(2, logFile("file1_1-0-1_002_1.deletes.parquet", 100)),
        new LsmFileGroupRecordIterator.LogReaderSpec(3, logFile("file1_1-0-1_003_1.log.parquet", 5)));

    Set<Integer> directReadersWithBase =
        LsmFileGroupRecordIterator.selectDirectLogMergeOrders(logReaderSpecs, true, 3);
    Set<Integer> directReadersWithoutBase =
        LsmFileGroupRecordIterator.selectDirectLogMergeOrders(logReaderSpecs, false, 2);

    assertEquals(new HashSet<>(Arrays.asList(2, 3)), directReadersWithBase);
    assertEquals(new HashSet<>(Arrays.asList(2, 3)), directReadersWithoutBase);
  }

  private static LsmFileGroupRecordIterator.SortedRunReader<String> sortedRunReader(int mergeOrder,
                                                                                    BufferedRecord<String>... records) {
    LsmFileGroupRecordIterator.SortedRunReader<String> reader = new LsmFileGroupRecordIterator.SortedRunReader<>(
        mergeOrder, ClosableIterator.wrap(Arrays.asList(records).iterator()));
    assertTrue(reader.advance());
    return reader;
  }

  private static BufferedRecord<String> record(String recordKey, String value) {
    return new BufferedRecord<>(recordKey, 1, value, null, null);
  }

  private static HoodieSchema tableSchema() {
    return HoodieSchema.createRecord("test_record", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
        HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG))));
  }

  private static List<String> drain(LsmFileGroupRecordIterator.LoserTree<String> loserTree) {
    List<String> records = new ArrayList<>();
    while (!loserTree.isEmpty()) {
      BufferedRecord<String> record = loserTree.popWinner();
      records.add(record.getRecordKey() + ":" + (record.isDelete() ? "DELETE" : record.getRecord()));
    }
    return records;
  }

  private static HoodieLogFile logFile(String fileName, long fileSize) {
    return new HoodieLogFile(new StoragePath("/tmp/" + fileName), fileSize);
  }
}
