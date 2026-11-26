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

package org.apache.hudi.table.format.cow.utils;

import org.apache.hudi.table.format.cow.vector.position.RowPosition;
import org.apache.hudi.table.format.cow.vector.type.ParquetGroupField;

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link NestedPositionUtil}.
 */
class TestNestedPositionUtil {

  /** An optional top-level row whose last field is an optional list of optional ints. */
  private static final ParquetGroupField ROW =
      new ParquetGroupField(RowType.of(new IntType()), 0, 1, false, Collections.emptyList());

  @Test
  void testRowOffsetsCountOneValuePerRowWhenTheLastFieldIsAnArray() {
    // two rows of three elements each, then the first element of the next row
    int[] definitionLevels = {4, 4, 4, 4, 4, 4, 4};
    int[] repetitionLevels = {0, 1, 1, 0, 1, 1, 0};

    RowPosition position = NestedPositionUtil.calculateRowOffsets(ROW, definitionLevels, repetitionLevels);

    assertEquals(3, position.getPositionsCount());
    assertNull(position.getIsNull());
  }

  @Test
  void testRowOffsetsWithNullRows() {
    // a row of three elements, a null row, then the first element of the next row
    int[] definitionLevels = {4, 4, 4, 0, 4};
    int[] repetitionLevels = {0, 1, 1, 0, 0};

    RowPosition position = NestedPositionUtil.calculateRowOffsets(ROW, definitionLevels, repetitionLevels);

    assertEquals(3, position.getPositionsCount());
    assertArrayEquals(new boolean[] {false, true, false}, position.getIsNull());
  }
}
