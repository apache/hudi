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

package org.apache.hudi.io.storage.row.parquet;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.util.HoodieSchemaConverter;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class TestParquetRowDataWriter {

  @Test
  void testWriteDecimalWithWidthFromHoodieSchema() {
    HoodieSchema schema = HoodieSchema.parse(
        "{\"type\":\"record\",\"name\":\"rec\",\"fields\":["
            + "{\"name\":\"large_fixed\",\"type\":{\"type\":\"fixed\",\"name\":\"large_fixed_type\","
            + "\"size\":10,\"logicalType\":\"decimal\",\"precision\":20,\"scale\":2}},"
            + "{\"name\":\"small_fixed\",\"type\":{\"type\":\"fixed\",\"name\":\"small_fixed_type\","
            + "\"size\":10,\"logicalType\":\"decimal\",\"precision\":10,\"scale\":2}},"
            + "{\"name\":\"bytes_decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
            + "\"precision\":20,\"scale\":2}}]}");
    BigDecimal largeValue = new BigDecimal("123456789.12");
    BigDecimal smallValue = new BigDecimal("-12.34");
    BigDecimal bytesValue = new BigDecimal("223456789.34");
    GenericRowData row = GenericRowData.of(
        DecimalData.fromBigDecimal(largeValue, 20, 2),
        DecimalData.fromBigDecimal(smallValue, 10, 2),
        DecimalData.fromBigDecimal(bytesValue, 20, 2));
    RecordConsumer consumer = mock(RecordConsumer.class);

    new ParquetRowDataWriter(consumer, true, schema).write(row);

    ArgumentCaptor<Binary> binaryCaptor = ArgumentCaptor.forClass(Binary.class);
    verify(consumer, times(3)).addBinary(binaryCaptor.capture());
    assertArrayEquals(signExtend(largeValue.unscaledValue().toByteArray(), 10),
        binaryCaptor.getAllValues().get(0).getBytes());
    assertArrayEquals(signExtend(smallValue.unscaledValue().toByteArray(), 10),
        binaryCaptor.getAllValues().get(1).getBytes());
    assertArrayEquals(signExtend(bytesValue.unscaledValue().toByteArray(), 9),
        binaryCaptor.getAllValues().get(2).getBytes());
  }

  @Test
  void testWritePrimitiveNestedArrayMapDecimalAndTimestampValues() {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("text", DataTypes.STRING()),
        DataTypes.FIELD("flag", DataTypes.BOOLEAN()),
        DataTypes.FIELD("bytes", DataTypes.BYTES()),
        DataTypes.FIELD("tiny", DataTypes.TINYINT()),
        DataTypes.FIELD("small", DataTypes.SMALLINT()),
        DataTypes.FIELD("number", DataTypes.INT()),
        DataTypes.FIELD("big", DataTypes.BIGINT()),
        DataTypes.FIELD("ratio", DataTypes.FLOAT()),
        DataTypes.FIELD("score", DataTypes.DOUBLE()),
        DataTypes.FIELD("day", DataTypes.DATE()),
        DataTypes.FIELD("time", DataTypes.TIME(3)),
        DataTypes.FIELD("small_decimal", DataTypes.DECIMAL(10, 2)),
        DataTypes.FIELD("large_decimal", DataTypes.DECIMAL(30, 4)),
        DataTypes.FIELD("timestamp3", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("timestamp6", DataTypes.TIMESTAMP_LTZ(6)),
        DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.STRING())),
        DataTypes.FIELD("empty_items", DataTypes.ARRAY(DataTypes.INT())),
        DataTypes.FIELD("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
        DataTypes.FIELD("nested", DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("label", DataTypes.STRING()),
            DataTypes.FIELD("optional", DataTypes.INT()))),
        DataTypes.FIELD("null_field", DataTypes.STRING()))
        .notNull().getLogicalType();
    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(rowType, "writer_record");

    Map<Object, Object> attributes = new LinkedHashMap<>();
    attributes.put(StringData.fromString("present"), 1);
    attributes.put(StringData.fromString("missing"), null);
    GenericRowData row = GenericRowData.of(
        StringData.fromString("hello"), true, new byte[] {1, 2},
        3, 4, 5, 6L, 7.5f, 8.25d, 9, 10,
        DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2),
        DecimalData.fromBigDecimal(new BigDecimal("12345678901234567890.1234"), 30, 4),
        TimestampData.fromInstant(Instant.parse("2025-02-03T04:05:06.123Z")),
        TimestampData.fromInstant(Instant.parse("2025-02-03T04:05:06.123456Z")),
        new GenericArrayData(new Object[] {StringData.fromString("a"), null, StringData.fromString("c")}),
        new GenericArrayData(new int[0]),
        new GenericMapData(attributes),
        GenericRowData.of(99L, StringData.fromString("nested"), null),
        null);

    RecordConsumer consumer = mock(RecordConsumer.class);
    new ParquetRowDataWriter(consumer, true, schema).write(row);

    verify(consumer).startMessage();
    verify(consumer).endMessage();
    verify(consumer, atLeastOnce()).startField(any(String.class), anyInt());
    verify(consumer, atLeastOnce()).addBoolean(true);
    verify(consumer, atLeastOnce()).addInteger(5);
    verify(consumer, atLeastOnce()).addLong(6L);
    verify(consumer, atLeastOnce()).addFloat(7.5f);
    verify(consumer, atLeastOnce()).addDouble(8.25d);
    verify(consumer, atLeastOnce()).addBinary(any());
    verify(consumer, atLeastOnce()).startGroup();
    verify(consumer, atLeastOnce()).endGroup();
    verify(consumer, never()).startField(eq("null_field"), anyInt());
  }

  @Test
  void testNonUtcTimestampWriter() {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("timestamp3", DataTypes.TIMESTAMP(3)),
        DataTypes.FIELD("timestamp6", DataTypes.TIMESTAMP(6))).notNull().getLogicalType();
    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(rowType, "timestamps");
    GenericRowData row = GenericRowData.of(
        TimestampData.fromInstant(Instant.parse("2025-02-03T04:05:06.123Z")),
        TimestampData.fromInstant(Instant.parse("2025-02-03T04:05:06.123456Z")));
    RecordConsumer consumer = mock(RecordConsumer.class);

    new ParquetRowDataWriter(consumer, false, schema).write(row);

    verify(consumer, times(2)).addLong(anyLong());
  }

  @Test
  void testArrayElementWritersForPrimitiveAndComplexTypes() {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("booleans", DataTypes.ARRAY(DataTypes.BOOLEAN())),
        DataTypes.FIELD("longs", DataTypes.ARRAY(DataTypes.BIGINT())),
        DataTypes.FIELD("floats", DataTypes.ARRAY(DataTypes.FLOAT())),
        DataTypes.FIELD("doubles", DataTypes.ARRAY(DataTypes.DOUBLE())),
        DataTypes.FIELD("binaries", DataTypes.ARRAY(DataTypes.BYTES())),
        DataTypes.FIELD("small_decimals", DataTypes.ARRAY(DataTypes.DECIMAL(10, 2))),
        DataTypes.FIELD("large_decimals", DataTypes.ARRAY(DataTypes.DECIMAL(30, 4))),
        DataTypes.FIELD("timestamps", DataTypes.ARRAY(DataTypes.TIMESTAMP(6))),
        DataTypes.FIELD("nested_arrays", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()))),
        DataTypes.FIELD("maps", DataTypes.ARRAY(
            DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))),
        DataTypes.FIELD("rows", DataTypes.ARRAY(DataTypes.ROW(
            DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("name", DataTypes.STRING()))))).notNull().getLogicalType();
    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(rowType, "array_elements");
    Map<Object, Object> map = new LinkedHashMap<>();
    map.put(StringData.fromString("key"), 1);
    GenericRowData row = GenericRowData.of(
        new GenericArrayData(new boolean[] {true, false}),
        new GenericArrayData(new long[] {1L, 2L}),
        new GenericArrayData(new float[] {1.5f, 2.5f}),
        new GenericArrayData(new double[] {3.5d, 4.5d}),
        new GenericArrayData(new Object[] {new byte[] {1}, new byte[] {2}}),
        new GenericArrayData(new Object[] {
            DecimalData.fromBigDecimal(new BigDecimal("12.34"), 10, 2)}),
        new GenericArrayData(new Object[] {
            DecimalData.fromBigDecimal(new BigDecimal("12345678901234567890.1234"), 30, 4)}),
        new GenericArrayData(new Object[] {
            TimestampData.fromInstant(Instant.parse("2025-02-03T04:05:06.123456Z"))}),
        new GenericArrayData(new Object[] {new GenericArrayData(new int[] {1, 2})}),
        new GenericArrayData(new Object[] {new GenericMapData(map)}),
        new GenericArrayData(new Object[] {
            GenericRowData.of(1L, StringData.fromString("nested"))}));
    RecordConsumer consumer = mock(RecordConsumer.class);

    new ParquetRowDataWriter(consumer, true, schema).write(row);

    verify(consumer, atLeastOnce()).addBoolean(true);
    verify(consumer, atLeastOnce()).addLong(1L);
    verify(consumer, atLeastOnce()).addFloat(1.5f);
    verify(consumer, atLeastOnce()).addDouble(3.5d);
    verify(consumer, atLeastOnce()).addBinary(any());
  }

  private static byte[] signExtend(byte[] bytes, int length) {
    byte[] result = new byte[length];
    Arrays.fill(result, 0, length - bytes.length, bytes[0] < 0 ? (byte) -1 : (byte) 0);
    System.arraycopy(bytes, 0, result, length - bytes.length, bytes.length);
    return result;
  }
}
