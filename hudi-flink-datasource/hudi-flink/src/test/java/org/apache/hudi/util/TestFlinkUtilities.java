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

package org.apache.hudi.util;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.format.CastMap;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestFlinkUtilities {

  @Test
  void testStateBackendConverterHandlesSupportedAndUnknownValues() {
    FlinkStateBackendConverter converter = new FlinkStateBackendConverter();

    assertInstanceOf(HashMapStateBackend.class, converter.convert("hashmap"));
    assertInstanceOf(EmbeddedRocksDBStateBackend.class, converter.convert("rocksdb"));
    HoodieException exception = assertThrows(HoodieException.class, () -> converter.convert("memory"));
    assertTrue(exception.getMessage().contains("memory"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testEmptyInputFormatContainsNoRecords() throws Exception {
    InputFormat<RowData, GenericInputSplit> inputFormat =
        (InputFormat<RowData, GenericInputSplit>) InputFormats.EMPTY_INPUT_FORMAT;
    GenericInputSplit[] splits = inputFormat.createInputSplits(2);

    assertEquals(1, splits.length);
    inputFormat.open(splits[0]);
    assertTrue(inputFormat.reachedEnd());
    assertThrows(NoSuchElementException.class, () -> inputFormat.nextRecord(null));
    inputFormat.close();
  }

  @Test
  void testRowDataProjectionPreservesKindNullsAndSelectedOrder() {
    RowType rowType = RowType.of(
        DataTypes.INT().getLogicalType(),
        DataTypes.STRING().getLogicalType());
    GenericRowData input = GenericRowData.of(7, null);
    input.setRowKind(RowKind.DELETE);

    RowDataProjection projection = RowDataProjection.instanceV2(rowType, new int[] {1, 0});
    RowData projected = projection.project(input);

    assertEquals(RowKind.DELETE, projected.getRowKind());
    assertTrue(projected.isNullAt(0));
    assertEquals(7, projected.getInt(1));
    assertArrayEquals(new Object[] {null, 7}, projection.projectAsValues(input));
  }

  @Test
  void testRowDataProjectionFactoriesAndValidation() {
    LogicalType[] types = {
        DataTypes.INT().getLogicalType(),
        DataTypes.STRING().getLogicalType()
    };
    RowType rowType = RowType.of(types);
    GenericRowData input = GenericRowData.of(3, StringData.fromString("value"));

    RowData projected = RowDataProjection.instance(rowType, new int[] {0, 1}).project(input);
    assertEquals(3, projected.getInt(0));
    assertEquals("value", projected.getString(1).toString());
    assertThrows(IllegalArgumentException.class,
        () -> RowDataProjection.instance(types, new int[] {0}));
  }

  @Test
  void testCastProjectionHandlesValuesAndNulls() {
    LogicalType[] types = {
        DataTypes.INT().getLogicalType(),
        DataTypes.STRING().getLogicalType()
    };
    RowDataCastProjection projection = new RowDataCastProjection(types, new CastMap());
    GenericRowData input = GenericRowData.of(11, null);

    RowData projected = projection.project(input);
    assertEquals(11, projected.getInt(0));
    assertTrue(projected.isNullAt(1));
  }

  @Test
  void testSharedModificationAndChangelogConstants() {
    assertTrue(ChangelogModes.FULL.contains(RowKind.UPDATE_BEFORE));
    assertFalse(ChangelogModes.UPSERT.contains(RowKind.UPDATE_BEFORE));
    assertTrue(ChangelogModes.UPSERT.contains(RowKind.DELETE));
    assertEquals(
        SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS,
        DataModificationInfos.DEFAULT_DELETE_INFO.getRowLevelDeleteMode());
    assertTrue(DataModificationInfos.DEFAULT_DELETE_INFO.requiredColumns().isEmpty());
    assertEquals(
        SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS,
        DataModificationInfos.DEFAULT_UPDATE_INFO.getRowLevelUpdateMode());
    assertTrue(DataModificationInfos.DEFAULT_UPDATE_INFO.requiredColumns().isEmpty());
  }

  @Test
  void testJsonDeserializationFunctionFactoriesAndLifecycle() throws Exception {
    RowType rowType = RowType.of(DataTypes.STRING().getLogicalType());
    assertInstanceOf(JsonDeserializationFunction.class, JsonDeserializationFunction.getInstance(rowType));

    JsonRowDataDeserializationSchema schema = mock(JsonRowDataDeserializationSchema.class);
    RowData expected = GenericRowData.of(StringData.fromString("value"));
    when(schema.deserialize(any(byte[].class))).thenReturn(expected);
    JsonDeserializationFunction function = new JsonDeserializationFunction(schema);

    function.open(new org.apache.flink.configuration.Configuration());
    assertSame(expected, function.map("{\"f0\":\"value\"}"));
    verify(schema).open(null);
  }
}
