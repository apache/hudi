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

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRowDataAvroRoundTrip {

  private static final RowType ROW_TYPE = (RowType) DataTypes.ROW(
      DataTypes.FIELD("tiny", DataTypes.TINYINT().notNull()),
      DataTypes.FIELD("small", DataTypes.SMALLINT().notNull()),
      DataTypes.FIELD("flag", DataTypes.BOOLEAN().notNull()),
      DataTypes.FIELD("number", DataTypes.INT().notNull()),
      DataTypes.FIELD("big", DataTypes.BIGINT().notNull()),
      DataTypes.FIELD("ratio", DataTypes.FLOAT().notNull()),
      DataTypes.FIELD("score", DataTypes.DOUBLE().notNull()),
      DataTypes.FIELD("day", DataTypes.DATE().notNull()),
      DataTypes.FIELD("time", DataTypes.TIME(3).notNull()),
      DataTypes.FIELD("name", DataTypes.STRING()),
      DataTypes.FIELD("payload", DataTypes.BYTES()),
      DataTypes.FIELD("amount", DataTypes.DECIMAL(20, 4)),
      DataTypes.FIELD("timestamp3", DataTypes.TIMESTAMP(3)),
      DataTypes.FIELD("timestamp6", DataTypes.TIMESTAMP(6)),
      DataTypes.FIELD("local_timestamp3", DataTypes.TIMESTAMP_LTZ(3)),
      DataTypes.FIELD("local_timestamp6", DataTypes.TIMESTAMP_LTZ(6)),
      DataTypes.FIELD("items", DataTypes.ARRAY(DataTypes.STRING())),
      DataTypes.FIELD("attributes", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())),
      DataTypes.FIELD("nested", DataTypes.ROW(
          DataTypes.FIELD("id", DataTypes.BIGINT()),
          DataTypes.FIELD("label", DataTypes.STRING()))),
      DataTypes.FIELD("nullable", DataTypes.STRING()))
      .notNull().getLogicalType();

  @Test
  public void testAllSupportedTypesRoundTripValueByValue() {
    Instant instant = Instant.parse("2025-02-03T04:05:06.123456Z");
    Map<Object, Object> attributes = new LinkedHashMap<>();
    attributes.put(StringData.fromString("one"), 1);
    attributes.put(StringData.fromString("missing"), null);
    GenericRowData input = GenericRowData.of(
        (byte) 1, (short) 2, true, 3, 4L, 5.5f, 6.25d, 20, 1234,
        StringData.fromString("alice"), new byte[] {7, 8},
        DecimalData.fromBigDecimal(new BigDecimal("123456789012.3400"), 20, 4),
        TimestampData.fromInstant(instant), TimestampData.fromInstant(instant),
        TimestampData.fromInstant(instant), TimestampData.fromInstant(instant),
        new GenericArrayData(new Object[] {StringData.fromString("a"), null, StringData.fromString("c")}),
        new GenericMapData(attributes),
        GenericRowData.of(99L, StringData.fromString("nested")),
        null);

    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(ROW_TYPE, "AllTypes");
    GenericRecord avro = (GenericRecord) RowDataToAvroConverters.createConverter(ROW_TYPE)
        .convert(schema, input);
    RowData output = (RowData) AvroToRowDataConverters
        .createRowConverter(schema, ROW_TYPE, true).convert(avro);
    RowData defaultOutput = (RowData) AvroToRowDataConverters
        .createRowConverter(ROW_TYPE).convert(avro);
    RowData nonUtcOutput = (RowData) AvroToRowDataConverters
        .createRowConverter(ROW_TYPE, false).convert(avro);
    RowData schemaOutput = (RowData) AvroToRowDataConverters
        .createRowConverter(schema).convert(avro);

    assertEquals((byte) 1, output.getByte(0));
    assertEquals((short) 2, output.getShort(1));
    assertTrue(output.getBoolean(2));
    assertEquals(3, output.getInt(3));
    assertEquals(4L, output.getLong(4));
    assertEquals(5.5f, output.getFloat(5));
    assertEquals(6.25d, output.getDouble(6));
    assertEquals(20, output.getInt(7));
    assertEquals(1234, output.getInt(8));
    assertEquals("alice", output.getString(9).toString());
    assertArrayEquals(new byte[] {7, 8}, output.getBinary(10));
    assertEquals(new BigDecimal("123456789012.3400"), output.getDecimal(11, 20, 4).toBigDecimal());
    assertEquals(instant.toEpochMilli(), output.getTimestamp(12, 3).getMillisecond());
    assertEquals(instant, output.getTimestamp(13, 6).toInstant());
    assertEquals(instant.toEpochMilli(), output.getTimestamp(14, 3).getMillisecond());
    assertEquals(instant, output.getTimestamp(15, 6).toInstant());
    assertEquals("a", output.getArray(16).getString(0).toString());
    assertTrue(output.getArray(16).isNullAt(1));
    assertEquals("c", output.getArray(16).getString(2).toString());
    assertMap(output.getMap(17));
    assertEquals(99L, output.getRow(18, 2).getLong(0));
    assertEquals("nested", output.getRow(18, 2).getString(1).toString());
    assertTrue(output.isNullAt(19));
    assertEquals("alice", defaultOutput.getString(9).toString());
    assertEquals("alice", nonUtcOutput.getString(9).toString());
    assertEquals("alice", schemaOutput.getString(9).toString());
  }

  @Test
  public void testPrimitiveRuntimeRepresentations() {
    HoodieSchema intSchema = HoodieSchemaConverter.convertToSchema(DataTypes.INT().getLogicalType());
    assertNull(AvroToRowDataConverters.createConverter(DataTypes.NULL().getLogicalType(), true)
        .convert("ignored"));
    assertEquals((byte) 3, AvroToRowDataConverters.createConverter(
        DataTypes.TINYINT().getLogicalType(), true).convert(3));
    assertEquals((short) 4, AvroToRowDataConverters.createConverter(
        DataTypes.SMALLINT().getLogicalType(), true).convert(4));
    assertEquals(5, AvroToRowDataConverters.createConverter(
        DataTypes.INT().getLogicalType(), true).convert(5));
    assertEquals(2, AvroToRowDataConverters.createConverter(
        DataTypes.DATE().getLogicalType(), true).convert(LocalDate.ofEpochDay(2)));
    assertEquals(1234, AvroToRowDataConverters.createConverter(
        DataTypes.TIME(3).getLogicalType(), true).convert(LocalTime.ofNanoOfDay(1_234_000_000L)));
    assertArrayEquals(new byte[] {1, 2}, (byte[]) AvroToRowDataConverters.createConverter(
        DataTypes.BYTES().getLogicalType(), true).convert(new byte[] {1, 2}));
    byte[] unscaled = new BigInteger("1234").toByteArray();
    assertEquals(new BigDecimal("12.34"), ((DecimalData) AvroToRowDataConverters.createConverter(
        DataTypes.DECIMAL(8, 2).getLogicalType(), true).convert(ByteBuffer.wrap(unscaled))).toBigDecimal());
    assertEquals(new BigDecimal("12.34"), ((DecimalData) AvroToRowDataConverters.createConverter(
        DataTypes.DECIMAL(8, 2).getLogicalType(), true).convert(unscaled)).toBigDecimal());
    assertEquals(7, RowDataToAvroConverters.createConverter(DataTypes.TINYINT().getLogicalType())
        .convert(intSchema, (byte) 7));
    assertEquals(8, RowDataToAvroConverters.createConverter(DataTypes.SMALLINT().getLogicalType())
        .convert(intSchema, (short) 8));
    assertNull(RowDataToAvroConverters.createConverter(DataTypes.NULL().getLogicalType())
        .convert(HoodieSchema.create(HoodieSchemaType.NULL), "ignored"));
    assertEquals(ByteBuffer.wrap(new byte[] {4, 5}), RowDataToAvroConverters.createConverter(
        DataTypes.BYTES().getLogicalType()).convert(
        HoodieSchemaConverter.convertToSchema(DataTypes.BYTES().getLogicalType()), new byte[] {4, 5}));
    HoodieSchema.Vector vectorSchema = HoodieSchema.createVector(2);
    Object vector = RowDataToAvroConverters.createConverter(
        DataTypes.ARRAY(DataTypes.FLOAT()).getLogicalType()).convert(
        vectorSchema, new GenericArrayData(new float[] {1.0f, 2.0f}));
    assertTrue(vector instanceof org.apache.avro.generic.GenericData.Fixed);
  }

  @Test
  public void testTimestampAndJodaRuntimeRepresentations() {
    Instant instant = Instant.parse("2025-02-03T04:05:06.123Z");
    assertEquals(instant, ((TimestampData) AvroToRowDataConverters.createConverter(
        DataTypes.TIMESTAMP(3).getLogicalType(), true).convert(instant)).toInstant());
    assertEquals(instant.toEpochMilli(), ((TimestampData) AvroToRowDataConverters.createConverter(
        DataTypes.TIMESTAMP(3).getLogicalType(), true).convert(new DateTime(instant.toEpochMilli())))
        .getMillisecond());
    TimestampData nonUtcTimestamp = (TimestampData) AvroToRowDataConverters.createConverter(
        DataTypes.TIMESTAMP(3).getLogicalType(), false).convert(instant.toEpochMilli());
    assertEquals(instant.toEpochMilli(), nonUtcTimestamp.toTimestamp().getTime());
    assertEquals((int) LocalDate.of(2025, 2, 3).toEpochDay(),
        AvroToRowDataConverters.createConverter(DataTypes.DATE().getLogicalType(), true)
            .convert(new org.joda.time.LocalDate(2025, 2, 3)));
    assertEquals(14_706_123,
        AvroToRowDataConverters.createConverter(DataTypes.TIME(3).getLogicalType(), true)
            .convert(new org.joda.time.LocalTime(4, 5, 6, 123)));
    assertSame(AvroToRowDataConverters.JodaConverter.getConverter(),
        AvroToRowDataConverters.JodaConverter.getConverter());
    assertThrows(IllegalArgumentException.class,
        () -> AvroToRowDataConverters.createConverter(DataTypes.TIMESTAMP(9).getLogicalType(), true));
  }

  private static void assertMap(MapData map) {
    ArrayData keys = map.keyArray();
    ArrayData values = map.valueArray();
    Map<String, Integer> actual = new LinkedHashMap<>();
    for (int i = 0; i < map.size(); i++) {
      actual.put(keys.getString(i).toString(), values.isNullAt(i) ? null : values.getInt(i));
    }
    Map<String, Integer> expected = new LinkedHashMap<>();
    expected.put("one", 1);
    expected.put("missing", null);
    assertEquals(expected, actual);
  }
}
