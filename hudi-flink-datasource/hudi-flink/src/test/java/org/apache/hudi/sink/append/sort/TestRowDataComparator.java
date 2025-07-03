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
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRowDataComparator {

  @Test
  void testSingleSortKey() {
    // Create a row type with string and int fields
    RowType rowType = RowType.of(
        new LogicalType[] {
            new VarCharType(),
            new IntType()
        },
        new String[] {"name", "age"}
    );

    // Test sorting by name
    RowDataComparator comparator = new RowDataComparator(rowType, "name");

    RowData row1 = GenericRowData.of(StringData.fromString("Alice"), 25);
    RowData row2 = GenericRowData.of(StringData.fromString("Bob"), 30);
    RowData row3 = GenericRowData.of(StringData.fromString("Alice"), 20);
    RowData row4 = GenericRowData.of((StringData) null, 35);

    // Test normal comparison
    assertTrue(comparator.compare(row1, row2) < 0);  // Alice < Bob
    assertTrue(comparator.compare(row2, row1) > 0);  // Bob > Alice
    assertEquals(0, comparator.compare(row1, row3)); // Alice == Alice

    // Test null handling
    assertTrue(comparator.compare(row4, row1) > 0);  // null > Alice
    assertTrue(comparator.compare(row1, row4) < 0);  // Alice < null
    assertEquals(0, comparator.compare(row4, row4)); // null == null

    // Test sorting by age
    comparator = new RowDataComparator(rowType, "age");

    assertTrue(comparator.compare(row1, row2) < 0);  // 25 < 30
    assertTrue(comparator.compare(row2, row1) > 0);  // 30 > 25
    assertEquals(1, comparator.compare(row1, row3)); // 25 == 25
  }

  @Test
  void testMultipleSortKeys() {
    // Create a row type with string, int, and double fields
    RowType rowType = RowType.of(
        new LogicalType[] {
            new VarCharType(),
            new IntType(),
            new DoubleType()
        },
        new String[] {"name", "age", "score"}
    );

    // Test sorting by name, then age
    RowDataComparator comparator = new RowDataComparator(rowType, "name,age");

    RowData row1 = GenericRowData.of(StringData.fromString("Alice"), 25, 85.5);
    RowData row2 = GenericRowData.of(StringData.fromString("Alice"), 30, 90.0);
    RowData row3 = GenericRowData.of(StringData.fromString("Bob"), 20, 95.0);
    RowData row4 = GenericRowData.of(StringData.fromString("Alice"), 25, 88.0);

    // Test primary key (name) comparison
    assertTrue(comparator.compare(row1, row3) < 0);  // Alice < Bob
    assertTrue(comparator.compare(row3, row1) > 0);  // Bob > Alice

    // Test secondary key (age) comparison when primary key is equal
    assertTrue(comparator.compare(row1, row2) < 0);  // Alice(25) < Alice(30)
    assertTrue(comparator.compare(row2, row1) > 0);  // Alice(30) > Alice(25)
    assertEquals(0, comparator.compare(row1, row4)); // Alice(25) == Alice(25)

    // Test sorting by age, then score
    comparator = new RowDataComparator(rowType, "age,score");

    assertTrue(comparator.compare(row3, row1) < 0);  // 20 < 25
    assertTrue(comparator.compare(row1, row2) < 0);  // 25 < 30
    assertTrue(comparator.compare(row1, row4) < 0);  // 25,85.5 < 25,88.0
  }

  @Test
  void testMixedTypesSortKeys() {
    // Create a row type with different field types
    RowType rowType = RowType.of(
        new LogicalType[] {
            new VarCharType(),
            new IntType(),
            new DoubleType(),
            new BigIntType()
        },
        new String[] {"name", "age", "score", "timestamp"}
    );

    // Test sorting by multiple fields of different types
    RowDataComparator comparator = new RowDataComparator(rowType, "name,age,score,timestamp");

    RowData row1 = GenericRowData.of(
        StringData.fromString("Alice"), 25, 85.5, 1000L);
    RowData row2 = GenericRowData.of(
        StringData.fromString("Alice"), 25, 85.5, 2000L);
    RowData row3 = GenericRowData.of(
        StringData.fromString("Alice"), 25, 90.0, 1000L);
    RowData row4 = GenericRowData.of(
        StringData.fromString("Alice"), 30, 85.5, 1000L);

    // Test comparison using all sort keys
    assertTrue(comparator.compare(row1, row2) < 0);  // timestamp: 1000 < 2000
    assertTrue(comparator.compare(row1, row3) < 0);  // score: 85.5 < 90.0
    assertTrue(comparator.compare(row1, row4) < 0);  // age: 25 < 30
    assertEquals(0, comparator.compare(row1, row1)); // all fields equal
  }

  @Test
  void testNullHandlingInMultipleKeys() {
    // Create a row type with string and int fields
    RowType rowType = RowType.of(
        new LogicalType[] {
            new VarCharType(),
            new IntType()
        },
        new String[] {"name", "age"}
    );

    RowDataComparator comparator = new RowDataComparator(rowType, "name,age");

    RowData row1 = GenericRowData.of(StringData.fromString("Alice"), 25);
    RowData row2 = GenericRowData.of((StringData) null, 25);
    RowData row3 = GenericRowData.of(StringData.fromString("Alice"), (Integer) null);
    RowData row4 = GenericRowData.of((StringData) null, (Integer) null);

    // Test null handling in primary key
    assertTrue(comparator.compare(row1, row2) < 0);  // Alice < null
    assertTrue(comparator.compare(row2, row1) > 0);  // null > Alice

    // Test null handling in secondary key when primary key is equal
    assertTrue(comparator.compare(row1, row3) < 0);  // 25 < null
    assertTrue(comparator.compare(row3, row1) > 0);  // null > 25

    // Test all nulls
    assertEquals(0, comparator.compare(row4, row4)); // null,null == null,null
  }
} 