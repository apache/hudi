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

package org.apache.hudi.table.format;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.storage.StorageConfiguration;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestFlinkRowDataReaderContext {
  private final StorageConfiguration<?> storageConfig = mock(StorageConfiguration.class);
  private final HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("TestRecord", null, null,
      java.util.Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
          HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN))
      ));
  private FlinkRowDataReaderContext readerContext;

  @BeforeEach
  void setUp() {
    List<ExpressionPredicates.Predicate> predicates = new ArrayList<>();
    when(tableConfig.populateMetaFields()).thenReturn(true);
    when(tableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"id"}));
    readerContext = new FlinkRowDataReaderContext(
        storageConfig, null, predicates, tableConfig, Option.empty());
  }

  @Test
  void testConstructEngineRecordWithFieldValues() {
    Object[] fieldVals = new Object[] {1, StringData.fromString("Alice"), true};
    RowData row = readerContext.getRecordContext().constructEngineRecord(SCHEMA, fieldVals);
    assertEquals(fieldVals[0], row.getInt(0));
    assertEquals(fieldVals[1], row.getString(1));
    assertEquals(fieldVals[2], row.getBoolean(2));
  }

  @Test
  void testConstructEngineRecordWithNoUpdates() {
    RowData base = createBaseRow(1, "Alice", true);
    BufferedRecord<RowData> record = new BufferedRecord<>("anyKey", 1, base, 1, null);
    Map<Integer, Object> updates = new HashMap<>();
    RowData result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);

    assertEquals(1, result.getInt(0));
    assertEquals("Alice", result.getString(1).toString());
    assertTrue(result.getBoolean(2));
  }

  @Test
  void testConstructEngineRecordWithUpdateOneField() {
    RowData base = createBaseRow(1, "Alice", true);
    BufferedRecord<RowData> record = new BufferedRecord<>("anyKey", 1, base, 1, null);
    Map<Integer, Object> updates = new HashMap<>();
    updates.put(1, StringData.fromString("Bob"));

    RowData result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);

    assertEquals(1, result.getInt(0)); // unchanged
    assertEquals("Bob", result.getString(1).toString()); // updated
    assertTrue(result.getBoolean(2)); // unchanged
  }

  @Test
  void testConstructEngineRecordWithUpdateAllFields() {
    RowData base = createBaseRow(1, "Alice", true);
    BufferedRecord<RowData> record = new BufferedRecord<>("anyKey", 1, base, 1, null);
    Map<Integer, Object> updates = new HashMap<>();
    updates.put(0, 42);
    updates.put(1, StringData.fromString("Zoe"));
    updates.put(2, false);
    RowData result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);

    assertEquals(42, result.getInt(0));
    assertEquals("Zoe", result.getString(1).toString());
    assertFalse(result.getBoolean(2));
  }

  @Test
  void testConstructEngineRecordWithNullUpdate() {
    RowData base = createBaseRow(5, "Eve", true);
    BufferedRecord<RowData> record = new BufferedRecord<>("anyKey", 1, base, 1, null);
    Map<Integer, Object> updates = new HashMap<>();
    updates.put(1, null);
    RowData result = readerContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, record);

    assertEquals(5, result.getInt(0));
    assertTrue(result.isNullAt(1));
    assertTrue(result.getBoolean(2));
  }

  private GenericRowData createBaseRow(int id, String name, boolean active) {
    return GenericRowData.of(id, StringData.fromString(name), active);
  }
}
