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

package org.apache.hudi.sink.bulk.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link SortOperatorGen}.
 */
class TestSortOperatorGen {

  @Test
  void testGeneratedRecordComparator() {
    RowType rowType = RowType.of(
        new LogicalType[] {
            new IntType(),
            new VarCharType(),
            new VarBinaryType(),
            new DecimalType(10, 2),
            new TimestampType(3)
        },
        new String[] {"id", "name", "bytes", "amount", "ts"});

    SortOperatorGen sortOperatorGen =
        new SortOperatorGen(rowType, new String[] {"id", "name", "bytes", "amount", "ts"});
    RecordComparator comparator = sortOperatorGen.generateRecordComparator("TestSortComparator")
        .newInstance(Thread.currentThread().getContextClassLoader());

    GenericRowData row1 = row(1, "a", new byte[] {1, 2}, "1.00", 1000);
    GenericRowData row2 = row(1, "a", new byte[] {1, 3}, "1.00", 1000);
    GenericRowData row3 = row(1, "a", new byte[] {1, 3}, "2.00", 1000);
    GenericRowData row4 = row(1, "a", new byte[] {1, 3}, "2.00", 2000);
    GenericRowData nullRow = row(null, "a", new byte[] {1, 3}, "2.00", 2000);

    assertTrue(comparator.compare(row1, row2) < 0);
    assertTrue(comparator.compare(row2, row3) < 0);
    assertTrue(comparator.compare(row3, row4) < 0);
    assertTrue(comparator.compare(nullRow, row1) > 0);
    assertTrue(comparator.compare(row1, nullRow) < 0);
    assertEquals(0, comparator.compare(row4, row4));
  }

  @Test
  void testGeneratedNormalizedKeyComputer() {
    RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
    NormalizedKeyComputer computer = new SortOperatorGen(rowType, new String[] {"id"})
        .generateNormalizedKeyComputer("TestSortComputer")
        .newInstance(Thread.currentThread().getContextClassLoader());
    MemorySegment segment1 = MemorySegmentFactory.wrap(new byte[computer.getNumKeyBytes()]);
    MemorySegment segment2 = MemorySegmentFactory.wrap(new byte[computer.getNumKeyBytes()]);

    computer.putKey(GenericRowData.of(1), segment1, 0);
    computer.putKey(GenericRowData.of(2), segment2, 0);

    assertTrue(computer.getNumKeyBytes() > 1);
    assertTrue(computer.isKeyFullyDetermines());
    assertTrue(computer.compareKey(segment1, 0, segment2, 0) < 0);
  }

  @Test
  void testGeneratedNormalizedKeyComputerWithNullsLast() {
    RowType rowType = RowType.of(new LogicalType[] {new IntType()}, new String[] {"id"});
    NormalizedKeyComputer computer = new SortOperatorGen(rowType, new String[] {"id"})
        .generateNormalizedKeyComputer("TestSortComputer")
        .newInstance(Thread.currentThread().getContextClassLoader());
    MemorySegment segment1 = MemorySegmentFactory.wrap(new byte[computer.getNumKeyBytes()]);
    MemorySegment segment2 = MemorySegmentFactory.wrap(new byte[computer.getNumKeyBytes()]);

    computer.putKey(GenericRowData.of(1), segment1, 0);
    computer.putKey(GenericRowData.of((Object) null), segment2, 0);

    assertTrue(computer.compareKey(segment1, 0, segment2, 0) < 0);
  }

  @Test
  void testGeneratedNormalizedKeyComputerStopsAfterVariableLengthPrefix() {
    RowType rowType = RowType.of(
        new LogicalType[] {new VarCharType(), new IntType()},
        new String[] {"name", "id"});
    NormalizedKeyComputer computer = new SortOperatorGen(rowType, new String[] {"name", "id"})
        .generateNormalizedKeyComputer("TestSortComputer")
        .newInstance(Thread.currentThread().getContextClassLoader());
    MemorySegment segment1 = MemorySegmentFactory.wrap(new byte[computer.getNumKeyBytes()]);
    MemorySegment segment2 = MemorySegmentFactory.wrap(new byte[computer.getNumKeyBytes()]);

    computer.putKey(GenericRowData.of(StringData.fromString("abcdefghx"), 2), segment1, 0);
    computer.putKey(GenericRowData.of(StringData.fromString("abcdefghy"), 1), segment2, 0);

    assertFalse(computer.isKeyFullyDetermines());
    assertEquals(0, computer.compareKey(segment1, 0, segment2, 0));
  }

  private static GenericRowData row(Integer id, String name, byte[] bytes, String amount, long timestampMillis) {
    return GenericRowData.of(
        id,
        StringData.fromString(name),
        bytes,
        DecimalData.fromBigDecimal(new BigDecimal(amount), 10, 2),
        TimestampData.fromEpochMillis(timestampMillis));
  }
}
