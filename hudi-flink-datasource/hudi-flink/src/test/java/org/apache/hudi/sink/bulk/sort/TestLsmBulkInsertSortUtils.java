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

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.bulk.LsmBulkInsertWriterHelper;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.RowDataKeyGens;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for LSM sort-row construction and {@link LsmBulkInsertSortUtils}. */
class TestLsmBulkInsertSortUtils {

  @Test
  void testActualCompositeRecordKeyAndPayloadAreRetained() {
    RowType rowType = RowType.of(
        new LogicalType[] {
            new VarCharType(), new VarCharType(), new VarCharType(), new VarCharType()
        },
        new String[] {"id1", "id2", "name", "partition"});
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id1,id2");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    RowDataKeyGen keyGen = RowDataKeyGens.instance(conf, rowType);
    List<RowData> rows = Arrays.asList(
        compositeRow("a", "10"),
        compositeRow("a", "2"),
        compositeRow("a,", "2"),
        compositeRow("a", String.valueOf((char) 0xE000)),
        compositeRow("a", new String(Character.toChars(0x1F600))));

    for (RowData row : rows) {
      RowData sortRow = LsmBulkInsertWriterHelper.rowWithPartitionAndKey("p1", row, keyGen);
      assertEquals("p1", sortRow.getString(0).toString());
      assertEquals(keyGen.getRecordKey(row), sortRow.getString(1).toString());
      assertSame(row, sortRow.getRow(2, rowType.getFieldCount()));
    }
  }

  @Test
  void testDecoratedRowsSortByShuffleAndEncodedRecordKey() {
    RowType rowType = RowType.of(
        new LogicalType[] {new BigIntType(), new VarCharType(), new VarCharType()},
        new String[] {"id", "name", "partition"});
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    RowDataKeyGen keyGen = RowDataKeyGens.instance(conf, rowType);
    RowType sortRowType = LsmBulkInsertSortUtils.sortRowType(rowType);

    RowData row10 = row(10L, "ten", "p1");
    RowData row2 = row(2L, "two", "p1");
    RowData rowOtherPartition = row(1L, "one", "p2");
    RowData sortRow10 = LsmBulkInsertWriterHelper.rowWithPartitionAndKey("p1", row10, keyGen);
    RowData sortRow2 = LsmBulkInsertWriterHelper.rowWithPartitionAndKey("p1", row2, keyGen);
    RowData sortRowOtherPartition =
        LsmBulkInsertWriterHelper.rowWithPartitionAndKey("p2", rowOtherPartition, keyGen);

    SortOperatorGen sortOperatorGen = LsmBulkInsertSortUtils.getLsmSorterGen(sortRowType);
    RecordComparator comparator = sortOperatorGen.generateRecordComparator("TestLsmBulkInsertComparator")
        .newInstance(Thread.currentThread().getContextClassLoader());

    assertTrue(comparator.compare(sortRow10, sortRow2) < 0);
    assertTrue(comparator.compare(sortRow2, sortRowOtherPartition) < 0);
    assertEquals("10", sortRow10.getString(1).toString());
    assertSame(row10, sortRow10.getRow(2, rowType.getFieldCount()));
  }

  @Test
  void testDuplicateKeysRemainDistinctPayloads() {
    RowType rowType = RowType.of(
        new LogicalType[] {new BigIntType(), new VarCharType(), new VarCharType()},
        new String[] {"id", "name", "partition"});
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "partition");
    RowDataKeyGen keyGen = RowDataKeyGens.instance(conf, rowType);

    RowData first = row(1L, "first", "p1");
    RowData second = row(1L, "second", "p1");
    RowData firstSortRow =
        LsmBulkInsertWriterHelper.rowWithPartitionAndKey("p1", first, keyGen);
    RowData secondSortRow =
        LsmBulkInsertWriterHelper.rowWithPartitionAndKey("p1", second, keyGen);

    assertEquals(firstSortRow.getString(1), secondSortRow.getString(1));
    assertSame(first, firstSortRow.getRow(2, rowType.getFieldCount()));
    assertSame(second, secondSortRow.getRow(2, rowType.getFieldCount()));
  }

  private static RowData row(long id, String name, String partition) {
    return GenericRowData.of(
        id,
        StringData.fromString(name),
        StringData.fromString(partition));
  }

  private static RowData compositeRow(String id1, String id2) {
    return GenericRowData.of(
        StringData.fromString(id1),
        StringData.fromString(id2),
        StringData.fromString("payload"),
        StringData.fromString("p1"));
  }

}
