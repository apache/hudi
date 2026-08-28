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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestRowDataUtils {

  @Test
  void testJavaAndFlinkValueConverters() {
    assertNull(RowDataUtils.NULL_GETTER.getFieldOrNull(GenericRowData.of(1)));
    assertNull(RowDataUtils.javaValFunc(DataTypes.NULL().getLogicalType(), true).apply("ignored"));
    assertEquals(7, RowDataUtils.javaValFunc(DataTypes.TINYINT().getLogicalType(), true).apply((byte) 7));
    assertEquals(8, RowDataUtils.javaValFunc(DataTypes.SMALLINT().getLogicalType(), true).apply((short) 8));
    assertEquals(2, RowDataUtils.javaValFunc(DataTypes.DATE().getLogicalType(), true).apply(2));
    assertEquals("text", RowDataUtils.javaValFunc(DataTypes.STRING().getLogicalType(), true)
        .apply(StringData.fromString("text")));
    assertArrayEquals(new byte[] {1, 2}, ((ByteBuffer) RowDataUtils.javaValFunc(
        DataTypes.BYTES().getLogicalType(), true).apply(new byte[] {1, 2})).array());
    assertEquals(new BigDecimal("12.30"), RowDataUtils.javaValFunc(
        DataTypes.DECIMAL(8, 2).getLogicalType(), true)
        .apply(DecimalData.fromBigDecimal(new BigDecimal("12.30"), 8, 2)));

    assertEquals((byte) 7, RowDataUtils.flinkValFunc(DataTypes.TINYINT().getLogicalType(), true).apply((byte) 7));
    assertEquals((short) 8, RowDataUtils.flinkValFunc(DataTypes.SMALLINT().getLogicalType(), true).apply((short) 8));
    assertEquals(3, RowDataUtils.flinkValFunc(DataTypes.DATE().getLogicalType(), true)
        .apply(LocalDate.ofEpochDay(3)));
    assertEquals("text", RowDataUtils.flinkValFunc(DataTypes.STRING().getLogicalType(), true)
        .apply("text").toString());
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {3, 4});
    assertSame(buffer, RowDataUtils.flinkValFunc(DataTypes.BYTES().getLogicalType(), true).apply(buffer));
    assertEquals(new BigDecimal("45.60"), ((DecimalData) RowDataUtils.flinkValFunc(
        DataTypes.DECIMAL(8, 2).getLogicalType(), true).apply(new BigDecimal("45.60"))).toBigDecimal());
  }

  @Test
  void testTimestampConvertersAtMillisAndMicros() {
    TimestampData timestamp = TimestampData.fromInstant(Instant.parse("2025-02-03T04:05:06.123456Z"));
    long millis = timestamp.toInstant().toEpochMilli();
    long micros = timestamp.toInstant().getEpochSecond() * 1_000_000 + 123456;

    assertEquals(millis, RowDataUtils.javaValFunc(DataTypes.TIMESTAMP_LTZ(3).getLogicalType(), true)
        .apply(timestamp));
    assertEquals(micros, RowDataUtils.javaValFunc(DataTypes.TIMESTAMP_LTZ(6).getLogicalType(), true)
        .apply(timestamp));
    assertEquals(millis, RowDataUtils.javaValFunc(DataTypes.TIMESTAMP(3).getLogicalType(), true)
        .apply(timestamp));
    assertEquals(micros, RowDataUtils.javaValFunc(DataTypes.TIMESTAMP(6).getLogicalType(), true)
        .apply(timestamp));
    long localMillis = (long) RowDataUtils.javaValFunc(
        DataTypes.TIMESTAMP(3).getLogicalType(), false).apply(timestamp);
    long localMicros = (long) RowDataUtils.javaValFunc(
        DataTypes.TIMESTAMP(6).getLogicalType(), false).apply(timestamp);

    assertEquals(Instant.ofEpochMilli(millis), ((TimestampData) RowDataUtils.flinkValFunc(
        DataTypes.TIMESTAMP_LTZ(3).getLogicalType(), true).apply(millis)).toInstant());
    assertEquals(timestamp.toInstant(), ((TimestampData) RowDataUtils.flinkValFunc(
        DataTypes.TIMESTAMP_LTZ(6).getLogicalType(), true).apply(micros)).toInstant());
    assertEquals(millis, ((TimestampData) RowDataUtils.flinkValFunc(
        DataTypes.TIMESTAMP(3).getLogicalType(), false).apply(millis)).toTimestamp().getTime());
    assertEquals(timestamp.toInstant(), ((TimestampData) RowDataUtils.flinkValFunc(
        DataTypes.TIMESTAMP(6).getLogicalType(), true).apply(micros)).toInstant());
    assertEquals(localMillis, RowDataUtils.javaValFunc(DataTypes.TIMESTAMP(3).getLogicalType(), false)
        .apply(RowDataUtils.flinkValFunc(DataTypes.TIMESTAMP(3).getLogicalType(), false).apply(localMillis)));
    assertEquals(localMicros, RowDataUtils.javaValFunc(DataTypes.TIMESTAMP(6).getLogicalType(), false)
        .apply(RowDataUtils.flinkValFunc(DataTypes.TIMESTAMP(6).getLogicalType(), false).apply(localMicros)));

    assertThrows(UnsupportedOperationException.class,
        () -> RowDataUtils.javaValFunc(DataTypes.TIMESTAMP(9).getLogicalType(), true));
    assertThrows(UnsupportedOperationException.class,
        () -> RowDataUtils.flinkValFunc(DataTypes.TIMESTAMP_LTZ(9).getLogicalType(), true));
  }

  @Test
  void testGenericConversionAndPrecision() {
    assertNull(RowDataUtils.convertValueToFlinkType(null));
    assertEquals("value", RowDataUtils.convertValueToFlinkType("value").toString());
    assertEquals(new BigDecimal("1.20"), ((DecimalData) RowDataUtils.convertValueToFlinkType(
        new BigDecimal("1.20"))).toBigDecimal());
    Timestamp timestamp = Timestamp.valueOf("2025-02-03 04:05:06.123456");
    assertEquals(timestamp, ((TimestampData) RowDataUtils.convertValueToFlinkType(timestamp)).toTimestamp());
    assertEquals(2, RowDataUtils.convertValueToFlinkType(LocalDate.ofEpochDay(2)));
    assertArrayEquals(new byte[] {9, 8},
        (byte[]) RowDataUtils.convertValueToFlinkType(ByteBuffer.wrap(new byte[] {9, 8})));
    Object marker = new Object();
    assertSame(marker, RowDataUtils.convertValueToFlinkType(marker));
    assertEquals(3, RowDataUtils.precision(new TimestampType(3)));
    assertEquals(6, RowDataUtils.precision(new LocalZonedTimestampType(6)));
    assertThrows(AssertionError.class,
        () -> RowDataUtils.precision(DataTypes.INT().getLogicalType()));
  }
}
