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

package org.apache.hudi.sink.utils;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.buffer.HeapMemorySegmentPool;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.RowDataKeyGens;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.operators.sort.BinaryInMemorySortBuffer;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link RecordKeySortKeyComputer}. */
class TestRecordKeySortKeyComputer {

  private static final long BUFFER_SIZE_BYTES = 16L * 1024 * 1024;

  @Test
  void testNormalizedKeyLengthAndOrdering() {
    RowType longRowType = rowType(new String[] {"key"}, new LogicalType[] {new BigIntType()});
    RowDataKeyGen longKeyGen = keyGen(longRowType, "key");
    RecordKeySortKeyComputer longComputer = new RecordKeySortKeyComputer(longKeyGen, 1);
    assertEquals(8, longComputer.getNumKeyBytes());
    assertComparison(longComputer, new RecordKeySortComparator(longKeyGen),
        GenericRowData.of(10L), GenericRowData.of(2L), false);

    RowType complexRowType = rowType(
        new String[] {"key1", "key2"},
        new LogicalType[] {new VarCharType(), new VarCharType()});
    RowDataKeyGen complexKeyGen = keyGen(complexRowType, "key1,key2");
    RecordKeySortKeyComputer complexComputer = new RecordKeySortKeyComputer(complexKeyGen, 2);
    assertEquals(16, complexComputer.getNumKeyBytes());
    assertEquals("value1,key2:value2",
        complexKeyGen.getRecordKeyForComparison(stringRow("value1", "value2")));

    // The first eight ASCII bytes collide, so the complete comparator resolves the order.
    assertComparison(longComputer, new RecordKeySortComparator(longKeyGen),
        GenericRowData.of(123456781L), GenericRowData.of(123456782L), true);

    MemorySegment segment = MemorySegmentFactory.allocateUnpooledSegment(16);
    longComputer.putKey(GenericRowData.of(10L), segment, 0);
    longComputer.putKey(GenericRowData.of(2L), segment, 8);
    assertTrue(longComputer.compareKey(segment, 0, segment, 8) < 0);
    longComputer.swapKey(segment, 0, segment, 8);
    assertTrue(longComputer.compareKey(segment, 0, segment, 8) > 0);
  }

  @Test
  void testNormalizedKeyPreservesJavaStringOrder() {
    RowType rowType = rowType(new String[] {"key"}, new LogicalType[] {new VarCharType()});
    RowDataKeyGen keyGen = keyGen(rowType, "key");
    RecordKeySortKeyComputer computer = new RecordKeySortKeyComputer(keyGen, 1);
    RecordKeySortComparator comparator = new RecordKeySortComparator(keyGen);

    assertComparison(computer, comparator, stringRow("10"), stringRow("2"), false);
    assertComparison(computer, comparator, stringRow("abc"), stringRow("abcd"), false);
    assertComparison(computer, comparator, stringRow("\u0000a"), stringRow("\u0000b"), false);
    String supplementaryCharacter = new String(Character.toChars(0x1F600));
    String privateUseCharacter = String.valueOf((char) 0xE000);
    assertComparison(computer, comparator,
        stringRow(supplementaryCharacter), stringRow(privateUseCharacter), false);
    assertComparison(computer, comparator, stringRow("abcdefgh1"), stringRow("abcdefgh2"), true);
  }

  @Test
  void testNormalizedKeyCompareMatchesStringCompareToAtEncodingBoundaries() {
    List<String> values = Arrays.asList(
        stringFromCodeUnits(0x0000),
        stringFromCodeUnits(0x0000) + "a",
        "a",
        "a" + stringFromCodeUnits(0x0000),
        "aa",
        "abcdefgh1",
        "abcdefgh2",
        stringFromCodeUnits(0x007F),
        stringFromCodeUnits(0x0080),
        stringFromCodeUnits(0x1FFF),
        stringFromCodeUnits(0x2000),
        stringFromCodeUnits(0xD7FF),
        stringFromCodeUnits(0xD800),
        stringFromCodeUnits(0xD800, 0xDC00),
        stringFromCodeUnits(0xD83D, 0xDE00),
        stringFromCodeUnits(0xE000),
        stringFromCodeUnits(0xFFFF));

    assertNormalizedKeyComparisonsMatchStringCompareTo(values);
  }

  @Test
  void testNormalizedKeyCompareMatchesStringCompareToForRandomUtf16Strings() {
    Random random = new Random(42);
    List<String> values = new ArrayList<>();
    for (int i = 0; i < 1_000; i++) {
      int length = random.nextInt(23) + 1;
      char[] chars = new char[length];
      for (int j = 0; j < length; j++) {
        chars[j] = (char) random.nextInt(Character.MAX_VALUE + 1);
      }
      values.add(new String(chars));
    }

    RowType rowType = rowType(new String[] {"key"}, new LogicalType[] {new VarCharType()});
    RowDataKeyGen keyGen = keyGen(rowType, "key");
    RecordKeySortKeyComputer computer = new RecordKeySortKeyComputer(keyGen, 1);
    RecordKeySortComparator comparator = new RecordKeySortComparator(keyGen);
    for (int i = 0; i < values.size(); i++) {
      String left = values.get(i);
      String right = values.get((i * 31 + 17) % values.size());
      assertNormalizedComparisonMatchesStringCompareTo(
          computer, comparator, keyGen, left, right);
    }
  }

  @Test
  void testComparisonKeyPreservesCompositeRecordKeyOrder() {
    RowType rowType = rowType(
        new String[] {"key1", "key2"},
        new LogicalType[] {new VarCharType(), new VarCharType()});
    RowDataKeyGen keyGen = keyGen(rowType, "key1,key2");
    List<RowData> rows = Arrays.asList(
        stringRow("a", "z"),
        stringRow("aa", "a"),
        stringRow("a,b", "c"),
        stringRow("a", "b,c"),
        stringRow("", "a"),
        GenericRowData.of(null, StringData.fromString("a")));

    for (RowData left : rows) {
      for (RowData right : rows) {
        int recordKeyResult = keyGen.getRecordKey(left).compareTo(keyGen.getRecordKey(right));
        int comparisonKeyResult = keyGen.getRecordKeyForComparison(left)
            .compareTo(keyGen.getRecordKeyForComparison(right));
        assertEquals(Integer.signum(recordKeyResult), Integer.signum(comparisonKeyResult));
      }
    }
  }

  @Test
  void testBinaryInMemorySortBufferOrdering() throws Exception {
    RowType longRowType = rowType(new String[] {"key"}, new LogicalType[] {new BigIntType()});
    assertBufferSort(longRowType, "key", Arrays.asList(
        GenericRowData.of(2L), GenericRowData.of(10L), GenericRowData.of(-1L), GenericRowData.of(100L)));

    RowType complexRowType = rowType(
        new String[] {"key1", "key2"},
        new LogicalType[] {new VarCharType(), new VarCharType()});
    assertBufferSort(complexRowType, "key1,key2", Arrays.asList(
        stringRow("same-prefix-2", "a"),
        stringRow("same-prefix-1", "z"),
        stringRow("same-prefix-1", "a")));
  }

  private static void assertComparison(
      RecordKeySortKeyComputer computer,
      RecordKeySortComparator comparator,
      RowData left,
      RowData right,
      boolean expectNormalizedKeyCollision) {
    int keyBytes = computer.getNumKeyBytes();
    MemorySegment leftKey = MemorySegmentFactory.allocateUnpooledSegment(keyBytes);
    MemorySegment rightKey = MemorySegmentFactory.allocateUnpooledSegment(keyBytes);
    computer.putKey(left, leftKey, 0);
    computer.putKey(right, rightKey, 0);

    int normalizedResult = computer.compareKey(leftKey, 0, rightKey, 0);
    assertEquals(expectNormalizedKeyCollision, normalizedResult == 0);
    int result = normalizedResult == 0 ? comparator.compare(left, right) : normalizedResult;
    assertEquals(Integer.signum(comparator.compare(left, right)), Integer.signum(result));
  }

  private static void assertNormalizedKeyComparisonsMatchStringCompareTo(List<String> values) {
    RowType rowType = rowType(new String[] {"key"}, new LogicalType[] {new VarCharType()});
    RowDataKeyGen keyGen = keyGen(rowType, "key");
    RecordKeySortKeyComputer computer = new RecordKeySortKeyComputer(keyGen, 1);
    RecordKeySortComparator comparator = new RecordKeySortComparator(keyGen);
    for (String left : values) {
      for (String right : values) {
        assertNormalizedComparisonMatchesStringCompareTo(
            computer, comparator, keyGen, left, right);
      }
    }
  }

  private static void assertNormalizedComparisonMatchesStringCompareTo(
      RecordKeySortKeyComputer computer,
      RecordKeySortComparator comparator,
      RowDataKeyGen keyGen,
      String left,
      String right) {
    RowData leftRow = stringRow(left);
    RowData rightRow = stringRow(right);
    int keyBytes = computer.getNumKeyBytes();
    MemorySegment leftKey = MemorySegmentFactory.allocateUnpooledSegment(keyBytes);
    MemorySegment rightKey = MemorySegmentFactory.allocateUnpooledSegment(keyBytes);
    computer.putKey(leftRow, leftKey, 0);
    computer.putKey(rightRow, rightKey, 0);

    int expected = keyGen.getRecordKeyForComparison(leftRow)
        .compareTo(keyGen.getRecordKeyForComparison(rightRow));
    int normalizedResult = computer.compareKey(leftKey, 0, rightKey, 0);
    if (normalizedResult != 0) {
      assertEquals(Integer.signum(expected), Integer.signum(normalizedResult),
          () -> "Normalized-key order differs for " + printable(left) + " and " + printable(right));
    }
    int resolvedResult = normalizedResult == 0
        ? comparator.compare(leftRow, rightRow)
        : normalizedResult;
    assertEquals(Integer.signum(expected), Integer.signum(resolvedResult),
        () -> "Resolved order differs for " + printable(left) + " and " + printable(right));
  }

  private static String printable(String value) {
    StringBuilder builder = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      builder.append(String.format("\\u%04X", (int) value.charAt(i)));
    }
    return builder.append('"').toString();
  }

  private static String stringFromCodeUnits(int... codeUnits) {
    char[] chars = new char[codeUnits.length];
    for (int i = 0; i < codeUnits.length; i++) {
      chars[i] = (char) codeUnits[i];
    }
    return new String(chars);
  }

  private static void assertBufferSort(RowType rowType, String recordKeyFields, List<RowData> rows)
      throws Exception {
    RowDataKeyGen keyGen = keyGen(rowType, recordKeyFields);
    int recordKeyFieldCount = recordKeyFields.split(",").length;
    BinaryInMemorySortBuffer buffer = BufferUtils.createBuffer(
        rowType,
        new HeapMemorySegmentPool(MemoryManager.DEFAULT_PAGE_SIZE, BUFFER_SIZE_BYTES),
        new RecordKeySortKeyComputer(keyGen, recordKeyFieldCount),
        new RecordKeySortComparator(keyGen));
    try {
      List<String> expected = new ArrayList<>();
      for (RowData row : rows) {
        assertTrue(buffer.write(row));
        expected.add(keyGen.getRecordKey(row));
      }
      Collections.sort(expected);

      new QuickSort().sort(buffer);
      MutableObjectIterator<BinaryRowData> iterator = buffer.getIterator();
      BinaryRowData reuse = new BinaryRowData(rowType.getFieldCount());
      List<String> actual = new ArrayList<>();
      BinaryRowData row;
      while ((row = iterator.next(reuse)) != null) {
        actual.add(keyGen.getRecordKey(row));
      }
      assertEquals(expected, actual);
    } finally {
      buffer.dispose();
    }
  }

  private static RowDataKeyGen keyGen(RowType rowType, String recordKeyFields) {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, recordKeyFields);
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "");
    return RowDataKeyGens.instance(conf, rowType);
  }

  private static RowType rowType(String[] names, LogicalType[] types) {
    return RowType.of(types, names);
  }

  private static RowData stringRow(String... values) {
    Object[] fields = Arrays.stream(values).map(StringData::fromString).toArray();
    return GenericRowData.of(fields);
  }
}
