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

package org.apache.hudi.sink.append.sort;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFieldComparators {

  @Test
  void testCreateFieldComparators() {
    // Create a row type with different field types
    RowType rowType = RowType.of(
        new LogicalType[] {
            new VarCharType(),
            new IntType(),
            new DoubleType(),
            new BigIntType(),
            new SmallIntType()
        },
        new String[] {"string_field", "int_field", "double_field", "long_field", "short_field"}
    );

    // Test creating comparators for all fields
    List<String> allFields = Arrays.asList("string_field", "int_field", "double_field", "long_field", "short_field");
    List<FieldComparators.FieldComparator> comparators = FieldComparators.createFieldComparators(rowType, allFields);
    assertEquals(5, comparators.size());
    assertTrue(comparators.get(0) instanceof FieldComparators.StringComparator);
    assertTrue(comparators.get(1) instanceof FieldComparators.IntegerComparator);
    assertTrue(comparators.get(2) instanceof FieldComparators.DoubleComparator);
    assertTrue(comparators.get(3) instanceof FieldComparators.LongComparator);
    assertTrue(comparators.get(4) instanceof FieldComparators.ShortComparator);

    // Test creating comparators for a subset of fields
    List<String> subsetFields = Arrays.asList("string_field", "double_field");
    comparators = FieldComparators.createFieldComparators(rowType, subsetFields);
    assertEquals(2, comparators.size());
    assertTrue(comparators.get(0) instanceof FieldComparators.StringComparator);
    assertTrue(comparators.get(1) instanceof FieldComparators.DoubleComparator);

    // Test invalid field name
    assertThrows(IllegalArgumentException.class, () ->
        FieldComparators.createFieldComparators(rowType, Arrays.asList("non_existent_field")));
  }

  @Test
  void testStringComparator() {
    FieldComparators.StringComparator comparator = new FieldComparators.StringComparator(0);
    RowData row1 = GenericRowData.of(StringData.fromString("abc"));
    RowData row2 = GenericRowData.of(StringData.fromString("def"));
    RowData row3 = GenericRowData.of(StringData.fromString("abc"));
    RowData row4 = GenericRowData.of((StringData) null);

    // Test normal comparison
    assertTrue(comparator.compare(row1, row2) < 0);
    assertTrue(comparator.compare(row2, row1) > 0);
    assertEquals(0, comparator.compare(row1, row3));

    // Test null handling
    assertTrue(comparator.compare(row4, row1) > 0);
    assertTrue(comparator.compare(row1, row4) < 0);
    assertEquals(0, comparator.compare(row4, row4));
  }

  @Test
  void testIntegerComparator() {
    FieldComparators.IntegerComparator comparator = new FieldComparators.IntegerComparator(0);
    RowData row1 = GenericRowData.of(1);
    RowData row2 = GenericRowData.of(2);
    RowData row3 = GenericRowData.of(1);

    assertTrue(comparator.compare(row1, row2) < 0);
    assertTrue(comparator.compare(row2, row1) > 0);
    assertEquals(0, comparator.compare(row1, row3));
  }

  @Test
  void testLongComparator() {
    FieldComparators.LongComparator comparator = new FieldComparators.LongComparator(0);
    RowData row1 = GenericRowData.of(1L);
    RowData row2 = GenericRowData.of(2L);
    RowData row3 = GenericRowData.of(1L);

    assertTrue(comparator.compare(row1, row2) < 0);
    assertTrue(comparator.compare(row2, row1) > 0);
    assertEquals(0, comparator.compare(row1, row3));
  }

  @Test
  void testFloatComparator() {
    FieldComparators.FloatComparator comparator = new FieldComparators.FloatComparator(0);
    RowData row1 = GenericRowData.of(1.0f);
    RowData row2 = GenericRowData.of(2.0f);
    RowData row3 = GenericRowData.of(1.0f);

    assertTrue(comparator.compare(row1, row2) < 0);
    assertTrue(comparator.compare(row2, row1) > 0);
    assertEquals(0, comparator.compare(row1, row3));
  }

  @Test
  void testDoubleComparator() {
    FieldComparators.DoubleComparator comparator = new FieldComparators.DoubleComparator(0);
    RowData row1 = GenericRowData.of(1.0);
    RowData row2 = GenericRowData.of(2.0);
    RowData row3 = GenericRowData.of(1.0);

    assertTrue(comparator.compare(row1, row2) < 0);
    assertTrue(comparator.compare(row2, row1) > 0);
    assertEquals(0, comparator.compare(row1, row3));
  }

  @Test
  void testShortComparator() {
    FieldComparators.ShortComparator comparator = new FieldComparators.ShortComparator(0);
    RowData row1 = GenericRowData.of((short) 1);
    RowData row2 = GenericRowData.of((short) 2);
    RowData row3 = GenericRowData.of((short) 1);

    assertTrue(comparator.compare(row1, row2) < 0);
    assertTrue(comparator.compare(row2, row1) > 0);
    assertEquals(0, comparator.compare(row1, row3));
  }

  @Test
  void testBinaryComparator() {
    FieldComparators.BinaryComparator comparator = new FieldComparators.BinaryComparator(0);
    byte[] bytes1 = new byte[] {1, 2, 3};
    byte[] bytes2 = new byte[] {1, 2, 4};
    byte[] bytes3 = new byte[] {1, 2, 3};

    RowData row1 = GenericRowData.of(bytes1);
    RowData row2 = GenericRowData.of(bytes2);
    RowData row3 = GenericRowData.of(bytes3);

    assertTrue(comparator.compare(row1, row2) < 0);
    assertTrue(comparator.compare(row2, row1) > 0);
    assertEquals(0, comparator.compare(row1, row3));
  }
} 