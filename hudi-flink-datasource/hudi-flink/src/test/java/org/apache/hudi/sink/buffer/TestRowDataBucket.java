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

package org.apache.hudi.sink.buffer;

import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.utils.TestData;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link RowDataBucket}. */
class TestRowDataBucket {

  @Test
  void testDivergedBucketCannotBeReusedAndReleasesAllPages() throws Exception {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("payload", DataTypes.VARCHAR(Integer.MAX_VALUE)))
        .notNull()
        .getLogicalType();
    HeapMemorySegmentPool pool = new HeapMemorySegmentPool(32 * 1024, 256 * 1024);
    int initialFreePages = pool.freePages();
    RowDataBucket bucket = new RowDataBucket(
        "bucket-0",
        BufferUtils.createBuffer(rowType, pool),
        new BucketInfo(BucketType.INSERT, "file-0", "partition-0"),
        256.0);

    List<String> expectedIds = new ArrayList<>();
    String payload = repeat('x', 64 * 1024);
    boolean writeFailed = false;
    try {
      for (int i = 0; i < 100; i++) {
        String id = "uuid-" + i;
        RowData row = TestData.insertRow(
            rowType, StringData.fromString(id), StringData.fromString(payload));
        if (!bucket.writeRow(row)) {
          writeFailed = true;
          break;
        }
        expectedIds.add(id);
      }

      assertTrue(writeFailed, "the tiny pool should cause BinaryInMemorySortBuffer.write to return false");
      assertTrue(bucket.isDiverged());
      assertFalse(bucket.isEmpty(), "successful rows written before exhaustion should remain readable");

      MutableObjectIterator<BinaryRowData> iterator = bucket.getDataIterator();
      BinaryRowData reuse = new BinaryRowData(rowType.getFieldCount());
      List<String> actualIds = new ArrayList<>();
      BinaryRowData row;
      while ((row = iterator.next(reuse)) != null) {
        actualIds.add(row.getString(0).toString());
        assertTrue(payload.equals(row.getString(1).toString()), "variable-length payload should remain intact");
      }
      assertEquals(expectedIds, actualIds, "only successfully indexed rows should be readable");

      RowData extraRow = TestData.insertRow(
          rowType, StringData.fromString("uuid-extra"), StringData.fromString(payload));
      assertThrows(IllegalStateException.class, () -> bucket.writeRow(extraRow));
    } finally {
      bucket.dispose();
    }

    assertEquals(initialFreePages, pool.freePages(), "disposing the diverged bucket should return every page");
  }

  private static String repeat(char value, int count) {
    return String.valueOf(value).repeat(count);
  }
}
