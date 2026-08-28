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

package org.apache.hudi.io.storage.row;

import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.storage.row.lance.HoodieFlinkLanceArrowUtils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieFlinkLanceArrowUtils}.
 */
public class TestHoodieFlinkLanceArrowUtils {

  @Test
  public void testTimestampSchemaRoundTripPreservesLocalTimezone() {
    RowType rowType = RowType.of(
        new LogicalType[] {new TimestampType(6), new LocalZonedTimestampType(6)},
        new String[] {"timestamp", "local_timestamp"});

    RowType roundTripped = HoodieFlinkLanceArrowUtils.toRowType(
        HoodieFlinkLanceArrowUtils.toArrowSchema(rowType));

    assertInstanceOf(TimestampType.class, roundTripped.getTypeAt(0));
    assertInstanceOf(LocalZonedTimestampType.class, roundTripped.getTypeAt(1));
  }

  @Test
  public void testTimestampReadNormalizesPreEpochMicros() {
    try (BufferAllocator allocator = new RootAllocator();
         TimeStampMicroVector vector = new TimeStampMicroVector(
             "ts",
             FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
             allocator)) {
      vector.setSafe(0, -1_234_567L);
      vector.setValueCount(1);

      RowData rowData = HoodieFlinkLanceArrowUtils.toRowData(
          RowType.of(new LogicalType[] {new TimestampType(6)}, new String[] {"ts"}),
          Collections.singletonList(vector),
          0);

      assertEquals(TimestampData.fromEpochMillis(-1235L, 433000), rowData.getTimestamp(0, 6));
    }
  }

  @Test
  public void testNestedSchemaRoundTrip() {
    RowType rowType = nestedRowType();

    RowType roundTripped = HoodieFlinkLanceArrowUtils.toRowType(
        HoodieFlinkLanceArrowUtils.toArrowSchema(rowType));

    assertEquals(rowType, roundTripped);
  }

  @Test
  public void testRejectsMapTypeWhenWritingSchema() {
    HoodieNotSupportedException exception = assertThrows(HoodieNotSupportedException.class,
        () -> HoodieFlinkLanceArrowUtils.toArrowSchema(RowType.of(
            new LogicalType[] {new MapType(new VarCharType(), new IntType())},
            new String[] {"attributes"})));
    assertTrue(exception.getMessage().contains("Flink Lance base-file support currently supports primitive, ROW, and ARRAY columns;"));
  }

  private static RowType nestedRowType() {
    RowType profileType = RowType.of(
        new LogicalType[] {new VarCharType(), new IntType()},
        new String[] {"name", "age"});
    ArrayType numbersType = new ArrayType(new IntType());
    ArrayType profilesType = new ArrayType(profileType);
    return RowType.of(
        new LogicalType[] {profileType, numbersType, profilesType},
        new String[] {"profile", "numbers", "profiles"});
  }
}
