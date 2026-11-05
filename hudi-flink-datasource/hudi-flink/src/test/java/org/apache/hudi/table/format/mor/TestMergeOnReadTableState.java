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

package org.apache.hudi.table.format.mor;

import org.apache.hudi.common.model.HoodieRecord;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class TestMergeOnReadTableState {

  @Test
  void testStateExposesSchemasSplitsAndRequiredPositions() {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("id", DataTypes.STRING()),
        DataTypes.FIELD(HoodieRecord.OPERATION_METADATA_FIELD, DataTypes.STRING()),
        DataTypes.FIELD("name", DataTypes.STRING())).getLogicalType();
    RowType requiredRowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("id", DataTypes.STRING())).getLogicalType();
    MergeOnReadTableState<String> state = new MergeOnReadTableState<>(
        rowType, requiredRowType, "table-schema", "required-schema", Collections.singletonList("split"));

    assertSame(rowType, state.getRowType());
    assertSame(requiredRowType, state.getRequiredRowType());
    assertEquals("table-schema", state.getTableSchema());
    assertEquals("required-schema", state.getRequiredSchema());
    assertEquals(Collections.singletonList("split"), state.getInputSplits());
    assertEquals(1, state.getOperationPos());
    assertArrayEquals(new int[] {2, 0}, state.getRequiredPositions());
  }

  @Test
  void testMissingRequiredFieldIsReportedAsNegativePosition() {
    RowType rowType = RowType.of(DataTypes.INT().getLogicalType(), DataTypes.STRING().getLogicalType());
    RowType required = new RowType(Collections.singletonList(
        new RowType.RowField("missing", DataTypes.STRING().getLogicalType())));
    MergeOnReadTableState<Integer> state =
        new MergeOnReadTableState<>(rowType, required, "schema", "required", Arrays.asList(1, 2));

    assertEquals(-1, state.getOperationPos());
    assertArrayEquals(new int[] {-1}, state.getRequiredPositions());
  }
}
