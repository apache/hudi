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
import org.apache.hudi.io.storage.row.lance.LanceRowDataWriter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link LanceRowDataWriter}.
 */
public class TestLanceRowDataWriter {

  @Test
  public void testPrimitiveValuesAndNulls() {
    RowType rowType = primitiveRowType();
    TimestampData timestamp = TimestampData.fromEpochMillis(1234L, 567000);
    TimestampData localTimestamp = TimestampData.fromEpochMillis(5678L, 123000);
    DecimalData decimal = DecimalData.fromBigDecimal(new BigDecimal("12345.67"), 10, 2);
    GenericRowData values = GenericRowData.of(
        true,
        (byte) 12,
        (short) 1234,
        123456,
        20000,
        12345678,
        1234567890123L,
        1.25F,
        2.5D,
        StringData.fromString("char"),
        StringData.fromString("varchar"),
        new byte[] {1, 2, 3},
        new byte[] {4, 5, 6, 7},
        decimal,
        timestamp,
        localTimestamp);
    GenericRowData nulls = new GenericRowData(rowType.getFieldCount());

    try (BufferAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(
             HoodieFlinkLanceArrowUtils.toArrowSchema(rowType), allocator)) {
      root.allocateNew();
      LanceRowDataWriter writer = new LanceRowDataWriter(rowType, root.getFieldVectors(), true);
      writer.write(values, 0);
      writer.write(nulls, 1);
      root.getFieldVectors().forEach(vector -> vector.setValueCount(2));
      root.setRowCount(2);

      RowData actual = HoodieFlinkLanceArrowUtils.toRowData(rowType, root.getFieldVectors(), 0);
      assertEquals(true, actual.getBoolean(0));
      assertEquals((byte) 12, actual.getByte(1));
      assertEquals((short) 1234, actual.getShort(2));
      assertEquals(123456, actual.getInt(3));
      assertEquals(20000, actual.getInt(4));
      assertEquals(12345678, actual.getInt(5));
      assertEquals(1234567890123L, actual.getLong(6));
      assertEquals(1.25F, actual.getFloat(7));
      assertEquals(2.5D, actual.getDouble(8));
      assertEquals(StringData.fromString("char"), actual.getString(9));
      assertEquals(StringData.fromString("varchar"), actual.getString(10));
      assertArrayEquals(new byte[] {1, 2, 3}, actual.getBinary(11));
      assertArrayEquals(new byte[] {4, 5, 6, 7}, actual.getBinary(12));
      assertEquals(decimal, actual.getDecimal(13, 10, 2));
      assertEquals(timestamp, actual.getTimestamp(14, 6));
      assertEquals(localTimestamp, actual.getTimestamp(15, 6));

      RowData actualNulls = HoodieFlinkLanceArrowUtils.toRowData(rowType, root.getFieldVectors(), 1);
      for (int i = 0; i < rowType.getFieldCount(); i++) {
        assertTrue(actualNulls.isNullAt(i));
      }
    }
  }

  @Test
  public void testTimestampWriteHonorsUtcTimestampFlag() {
    TimestampData timestampData = TimestampData.fromEpochMillis(1234L, 567000);
    GenericRowData rowData = GenericRowData.of(timestampData);

    try (BufferAllocator allocator = new RootAllocator();
         TimeStampMicroVector vector = new TimeStampMicroVector(
             "ts",
             FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)),
             allocator)) {
      RowType rowType = RowType.of(new LogicalType[] {new TimestampType(6)}, new String[] {"ts"});
      new LanceRowDataWriter(rowType, Collections.singletonList(vector), true).write(rowData, 0);
      assertEquals(1234567L, vector.get(0));

      new LanceRowDataWriter(rowType, Collections.singletonList(vector), false).write(rowData, 1);
      assertEquals(timestampData.toTimestamp().getTime() * 1000L, vector.get(1));
    }
  }

  @Test
  public void testNestedValueRoundTripAndListValueCount() {
    RowType rowType = nestedRowType();
    GenericRowData first = GenericRowData.of(
        GenericRowData.of(
            StringData.fromString("alice"),
            null,
            new GenericArrayData(new Object[] {StringData.fromString("primary"), null})),
        new GenericArrayData(new Object[] {1, null}),
        new GenericArrayData(new Object[] {
            GenericRowData.of(
                StringData.fromString("child"),
                3,
                new GenericArrayData(new Object[0])),
            null}),
        new GenericArrayData(new Object[] {
            new GenericArrayData(new Object[] {1, null}),
            null,
            new GenericArrayData(new Object[0])}));
    GenericRowData second = GenericRowData.of(
        null,
        new GenericArrayData(new Object[0]),
        new GenericArrayData(new Object[0]),
        new GenericArrayData(new Object[0]));
    GenericRowData third = new GenericRowData(rowType.getFieldCount());

    try (BufferAllocator allocator = new RootAllocator();
         VectorSchemaRoot root = VectorSchemaRoot.create(
             HoodieFlinkLanceArrowUtils.toArrowSchema(rowType), allocator)) {
      root.allocateNew();
      LanceRowDataWriter writer = new LanceRowDataWriter(rowType, root.getFieldVectors(), true);
      writer.write(first, 0);
      writer.write(second, 1);
      writer.write(third, 2);
      root.getFieldVectors().forEach(vector -> vector.setValueCount(3));
      root.setRowCount(3);

      assertEquals(first, HoodieFlinkLanceArrowUtils.toRowData(rowType, root.getFieldVectors(), 0));
      assertEquals(second, HoodieFlinkLanceArrowUtils.toRowData(rowType, root.getFieldVectors(), 1));
      assertEquals(third, HoodieFlinkLanceArrowUtils.toRowData(rowType, root.getFieldVectors(), 2));
      assertEquals(2, ((ListVector) root.getVector(1)).getDataVector().getValueCount());
      assertEquals(2, ((ListVector) ((StructVector) root.getVector(0)).getChild("tags"))
          .getDataVector().getValueCount());

      ListVector matrixVector = (ListVector) root.getVector(3);
      assertEquals(3, matrixVector.getDataVector().getValueCount());
      assertEquals(2, ((ListVector) matrixVector.getDataVector()).getDataVector().getValueCount());
    }
  }

  @Test
  public void testRejectsMapType() {
    MapType mapType = new MapType(new VarCharType(), new IntType());

    try (BufferAllocator allocator = new RootAllocator();
         IntVector vector = new IntVector("attributes", allocator)) {
      HoodieNotSupportedException exception = assertThrows(HoodieNotSupportedException.class,
          () -> new LanceRowDataWriter(
              RowType.of(new LogicalType[] {mapType}, new String[] {"attributes"}),
              Collections.singletonList(vector),
              true));
      assertTrue(exception.getMessage().contains(mapUnsupportedMessage()));
    }
  }

  private static String mapUnsupportedMessage() {
    return "Flink Lance base-file support currently supports primitive, ROW, and ARRAY columns;";
  }

  private static RowType nestedRowType() {
    ArrayType tagsType = new ArrayType(new VarCharType());
    RowType profileType = RowType.of(
        new LogicalType[] {new VarCharType(), new IntType(), tagsType},
        new String[] {"name", "age", "tags"});
    ArrayType numbersType = new ArrayType(new IntType());
    ArrayType profilesType = new ArrayType(profileType);
    ArrayType matrixType = new ArrayType(numbersType);
    return RowType.of(
        new LogicalType[] {profileType, numbersType, profilesType, matrixType},
        new String[] {"profile", "numbers", "profiles", "matrix"});
  }

  private static RowType primitiveRowType() {
    return RowType.of(
        new LogicalType[] {
            new BooleanType(),
            new TinyIntType(),
            new SmallIntType(),
            new IntType(),
            new DateType(),
            new TimeType(),
            new BigIntType(),
            new FloatType(),
            new DoubleType(),
            new CharType(4),
            new VarCharType(),
            new BinaryType(3),
            new VarBinaryType(),
            new DecimalType(10, 2),
            new TimestampType(6),
            new LocalZonedTimestampType(6)},
        new String[] {
            "boolean_value",
            "tinyint_value",
            "smallint_value",
            "int_value",
            "date_value",
            "time_value",
            "bigint_value",
            "float_value",
            "double_value",
            "char_value",
            "varchar_value",
            "binary_value",
            "varbinary_value",
            "decimal_value",
            "timestamp_value",
            "local_timestamp_value"});
  }
}
