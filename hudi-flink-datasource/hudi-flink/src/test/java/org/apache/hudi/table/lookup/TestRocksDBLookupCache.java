/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.lookup;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link RocksDBLookupCache}.
 */
class TestRocksDBLookupCache {

  @TempDir
  File tempFile;

  @Test
  void testPutGetClearAndClose() throws Exception {
    RowType keyType = RowType.of(
        new VarCharType(VarCharType.MAX_LENGTH));
    RowType rowType = RowType.of(
        new VarCharType(VarCharType.MAX_LENGTH),
        new IntType());
    TypeSerializer<RowData> keySerializer = InternalSerializers.create(keyType);
    TypeSerializer<RowData> rowSerializer = InternalSerializers.create(rowType);
    RocksDBLookupCache cache =
        new RocksDBLookupCache(keySerializer, rowSerializer, tempFile.getAbsolutePath());

    RowData key1 = key("id1");
    cache.addRow(key1, row("id1", 10));
    cache.addRow(key1, row("id1", 20));
    cache.addRow(key("id2"), row("id2", 30));

    List<RowData> rows = cache.getRows(key1);
    assertEquals(2, rows.size());
    assertEquals("id1", rows.get(0).getString(0).toString());
    assertEquals(10, rows.get(0).getInt(1));
    assertEquals(20, rows.get(1).getInt(1));
    assertNull(cache.getRows(key("missing")));

    cache.clear();
    assertNull(cache.getRows(key1));
    cache.addRow(key1, row("id1", 40));
    assertEquals(40, cache.getRows(key1).get(0).getInt(1));

    cache.close();
    cache.close();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 1024, 2051})
  void testBatchedWrites(int rowCount) throws Exception {
    RowType keyType = RowType.of(new VarCharType(VarCharType.MAX_LENGTH));
    RowType rowType = RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType());
    try (RocksDBLookupCache cache = new RocksDBLookupCache(
        InternalSerializers.create(keyType), InternalSerializers.create(rowType), tempFile.getAbsolutePath())) {
      RowData lookupKey = key("维度");
      GenericRowData reuse = GenericRowData.of(StringData.fromString("维度"), 0);
      for (int i = 0; i < rowCount; i++) {
        reuse.setField(1, i);
        cache.addRow(lookupKey, reuse);
      }
      reuse.setField(1, -1);
      List<RowData> rows = cache.getRows(lookupKey);
      assertEquals(IntStream.range(0, rowCount).boxed().collect(Collectors.toList()),
          rows.stream().map(row -> row.getInt(1)).sorted().collect(Collectors.toList()));

      cache.addRow(lookupKey, row("维度", rowCount));
      cache.flush();
      cache.flush();
      assertEquals(rowCount + 1, cache.getRows(lookupKey).size());

      // Clear must discard both persisted rows and the unflushed tail of a load.
      cache.addRow(key("pending"), row("pending", 1));
      cache.clear();
      assertNull(cache.getRows(lookupKey));
      assertNull(cache.getRows(key("pending")));
      cache.addRow(lookupKey, row("维度", -1));
      assertEquals(-1, cache.getRows(lookupKey).get(0).getInt(1));
    }
  }

  @Test
  void testLargeRows() throws Exception {
    RowType keyType = RowType.of(new VarCharType(VarCharType.MAX_LENGTH));
    RowType rowType = RowType.of(new VarCharType(VarCharType.MAX_LENGTH), new IntType());
    String value = String.join("", Collections.nCopies(1024 * 1024, "x"));
    try (RocksDBLookupCache cache = new RocksDBLookupCache(
        InternalSerializers.create(keyType), InternalSerializers.create(rowType), tempFile.getAbsolutePath())) {
      cache.addRow(key("large"), row(value, 1));
      cache.addRow(key("large"), row("tail", 2));
      List<RowData> rows = cache.getRows(key("large"));
      assertEquals(2, rows.size());
      assertEquals(value, rows.get(0).getString(0).toString());
      assertEquals("tail", rows.get(1).getString(0).toString());
    }
  }

  private static RowData key(String key) {
    return GenericRowData.of(StringData.fromString(key));
  }

  private static RowData row(String key, int value) {
    return GenericRowData.of(StringData.fromString(key), value);
  }
}
