/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.metadata.stats;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.generic.GenericData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestValueType {

  @Test
  public void testValueTypeNumbering() {
    // DO NOT MODIFY THE ORDERING OF THE TYPE NUMBERING
    assertEquals(0, ValueType.V1.ordinal());
    assertEquals(1, ValueType.NULL.ordinal());
    assertEquals(2, ValueType.BOOLEAN.ordinal());
    assertEquals(3, ValueType.INT.ordinal());
    assertEquals(4, ValueType.LONG.ordinal());
    assertEquals(5, ValueType.FLOAT.ordinal());
    assertEquals(6, ValueType.DOUBLE.ordinal());
    assertEquals(7, ValueType.STRING.ordinal());
    assertEquals(8, ValueType.BYTES.ordinal());
    assertEquals(9, ValueType.FIXED.ordinal());
    assertEquals(10, ValueType.DECIMAL.ordinal());
    assertEquals(11, ValueType.UUID.ordinal());
    assertEquals(12, ValueType.DATE.ordinal());
    assertEquals(13, ValueType.TIME_MILLIS.ordinal());
    assertEquals(14, ValueType.TIME_MICROS.ordinal());
    assertEquals(15, ValueType.TIMESTAMP_MILLIS.ordinal());
    assertEquals(16, ValueType.TIMESTAMP_MICROS.ordinal());
    assertEquals(17, ValueType.TIMESTAMP_NANOS.ordinal());
    assertEquals(18, ValueType.LOCAL_TIMESTAMP_MILLIS.ordinal());
    assertEquals(19, ValueType.LOCAL_TIMESTAMP_MICROS.ordinal());
    assertEquals(20, ValueType.LOCAL_TIMESTAMP_NANOS.ordinal());
    // IF YOU ADD A NEW TYPE, ADD IT TO THE END AND INCREMENT THE COUNT
    // AND ALSO ASSERT IT HERE SO THAT SOMEONE DOESN'T MESS WITH IT
    // IN THE FUTURE
    assertEquals(21, ValueType.values().length);
  }

  @Test
  public void testFromOrdinalRoundTrips() {
    for (ValueType type : ValueType.values()) {
      assertSame(type, ValueType.fromOrdinal(type.ordinal()));
    }
  }

  private static Comparable<?> standardize(ValueType type, Object val) {
    return type.standardizeJavaTypeAndPromote(val, ValueMetadata.NULL_METADATA);
  }

  @Test
  public void testCastToInteger() {
    assertNull(standardize(ValueType.INT, null));
    assertEquals(7, standardize(ValueType.INT, 7));
    assertEquals(1, standardize(ValueType.INT, Boolean.TRUE));
    assertEquals(0, standardize(ValueType.INT, Boolean.FALSE));
    // best effort parse from a string representation
    assertEquals(42, standardize(ValueType.INT, "42"));
  }

  @Test
  public void testCastToLong() {
    assertEquals(5L, standardize(ValueType.LONG, 5));
    assertEquals(9L, standardize(ValueType.LONG, 9L));
    assertEquals(1L, standardize(ValueType.LONG, Boolean.TRUE));
    assertEquals(123L, standardize(ValueType.LONG, "123"));
  }

  @Test
  public void testCastToFloatAndDouble() {
    assertEquals(3.0f, standardize(ValueType.FLOAT, 3));
    assertEquals(4.0f, standardize(ValueType.FLOAT, 4L));
    assertEquals(2.5f, standardize(ValueType.FLOAT, 2.5f));
    assertEquals(1.0f, standardize(ValueType.FLOAT, Boolean.TRUE));
    assertEquals(6.0d, standardize(ValueType.DOUBLE, 6));
    assertEquals(7.0d, standardize(ValueType.DOUBLE, 7L));
    assertEquals(0.0d, standardize(ValueType.DOUBLE, Boolean.FALSE));
    assertEquals(8.25d, standardize(ValueType.DOUBLE, 8.25d));
  }

  @Test
  public void testCastToBoolean() {
    assertEquals(Boolean.TRUE, ValueType.BOOLEAN.standardizeJavaTypeAndPromote(true, ValueMetadata.NULL_METADATA));
    assertThrows(UnsupportedOperationException.class,
        () -> ValueType.BOOLEAN.standardizeJavaTypeAndPromote("nope", ValueMetadata.NULL_METADATA));
  }

  @Test
  public void testCastToString() {
    assertEquals("abc", ValueType.castToString("abc"));
    assertEquals("11", ValueType.castToString(11));
    assertEquals("true", ValueType.castToString(Boolean.TRUE));
    assertEquals("bin", ValueType.castToString(Binary.fromString("bin")));
    assertThrows(UnsupportedOperationException.class, () -> ValueType.castToString(new Object()));
  }

  @Test
  public void testCastToBytesFromVariousSources() {
    byte[] raw = "hello".getBytes(StandardCharsets.UTF_8);
    assertEquals(ByteBuffer.wrap(raw), ValueType.castToBytes(ByteBuffer.wrap(raw)));
    assertEquals(ByteBuffer.wrap(raw), ValueType.castToBytes(raw));
    assertEquals(ByteBuffer.wrap(raw), ValueType.castToBytes(Binary.fromConstantByteArray(raw)));
    assertEquals(ByteBuffer.wrap(raw), ValueType.castToBytes("hello"));
    assertThrows(UnsupportedOperationException.class, () -> ValueType.castToBytes(new Object()));
  }

  @Test
  public void testCastToFixedRejectsString() {
    byte[] raw = {1, 2, 3};
    assertEquals(ByteBuffer.wrap(raw), ValueType.castToFixed(raw));
    // castToFixed, unlike castToBytes, does not accept String
    assertThrows(UnsupportedOperationException.class, () -> ValueType.castToFixed("abc"));
  }

  @Test
  public void testDecimalRoundTrip() {
    ValueMetadata.DecimalMetadata meta = ValueMetadata.DecimalMetadata.create(10, 2);
    BigDecimal value = new BigDecimal("12.34");
    // fromDecimal produces the primitive representation, toDecimal reverses it
    ByteBuffer primitive = ValueType.fromDecimal(value, meta);
    BigDecimal roundTripped = ValueType.toDecimal(primitive, meta);
    assertEquals(value, roundTripped);
    // castToDecimal accepts an already-typed BigDecimal unchanged
    assertEquals(value, ValueType.castToDecimal(value, meta));
    // integer input scaled by the metadata scale
    assertEquals(new BigDecimal("1.00"), ValueType.castToDecimal(100, meta));
    assertThrows(UnsupportedOperationException.class, () -> ValueType.castToDecimal(new Object(), meta));
  }

  @Test
  public void testUuidRoundTrip() {
    UUID uuid = UUID.fromString("12345678-1234-1234-1234-1234567890ab");
    assertEquals(uuid, ValueType.castToUUID(uuid, ValueMetadata.NULL_METADATA));
    assertEquals(uuid, ValueType.castToUUID(uuid.toString(), ValueMetadata.NULL_METADATA));
    String primitive = ValueType.fromUUID(uuid, ValueMetadata.NULL_METADATA);
    assertEquals(uuid, ValueType.toUUID(primitive, ValueMetadata.NULL_METADATA));
    assertThrows(UnsupportedOperationException.class,
        () -> ValueType.castToUUID(1, ValueMetadata.NULL_METADATA));
  }

  @Test
  public void testDateRoundTrip() {
    LocalDate date = LocalDate.of(2020, 1, 2);
    int epochDay = (int) date.toEpochDay();
    assertEquals(date, ValueType.castToDate(date, ValueMetadata.NULL_METADATA));
    assertEquals(date, ValueType.castToDate(epochDay, ValueMetadata.NULL_METADATA));
    assertEquals(date, ValueType.castToDate(java.sql.Date.valueOf(date), ValueMetadata.NULL_METADATA));
    assertEquals(epochDay, ValueType.fromDate(date, ValueMetadata.NULL_METADATA));
    assertEquals(date, ValueType.toDate(epochDay, ValueMetadata.NULL_METADATA));
    assertThrows(UnsupportedOperationException.class,
        () -> ValueType.castToDate("2020-01-02", ValueMetadata.NULL_METADATA));
  }

  @Test
  public void testTimeMillisAndMicrosRoundTrip() {
    LocalTime time = LocalTime.of(1, 2, 3, 4_000_000);
    int millisOfDay = time.toSecondOfDay() * 1000 + time.getNano() / 1_000_000;
    assertEquals(time, ValueType.castToTimeMillis(time, ValueMetadata.NULL_METADATA));
    assertEquals(time, ValueType.castToTimeMillis(millisOfDay, ValueMetadata.NULL_METADATA));
    assertEquals(millisOfDay, ValueType.fromTimeMillis(time, ValueMetadata.NULL_METADATA));
    assertEquals(time, ValueType.toTimeMillis(millisOfDay, ValueMetadata.NULL_METADATA));

    LocalTime microTime = LocalTime.of(4, 5, 6, 7_000);
    long microsOfDay = microTime.toSecondOfDay() * 1_000_000L + microTime.getNano() / 1_000;
    assertEquals(microTime, ValueType.castToTimeMicros(microsOfDay, ValueMetadata.NULL_METADATA));
    assertEquals(microsOfDay, ValueType.fromTimeMicros(microTime, ValueMetadata.NULL_METADATA));
  }

  @Test
  public void testTimestampMillisMicrosNanosRoundTrip() {
    Instant instant = Instant.ofEpochMilli(1_600_000_000_123L);
    assertEquals(instant, ValueType.castToTimestampMillis(instant, ValueMetadata.NULL_METADATA));
    assertEquals(instant, ValueType.castToTimestampMillis(Timestamp.from(instant), ValueMetadata.NULL_METADATA));
    assertEquals(instant, ValueType.castToTimestampMillis(instant.toEpochMilli(), ValueMetadata.NULL_METADATA));
    assertEquals(instant.toEpochMilli(), ValueType.fromTimestampMillis(instant, ValueMetadata.NULL_METADATA));
    assertEquals(instant, ValueType.toTimestampMillis(instant.toEpochMilli(), ValueMetadata.NULL_METADATA));

    Instant micros = Instant.ofEpochSecond(1_600_000_000L, 123_000L);
    long microVal = ValueType.fromTimestampMicros(micros, ValueMetadata.NULL_METADATA);
    assertEquals(micros, ValueType.toTimestampMicros(microVal, ValueMetadata.NULL_METADATA));

    Instant nanos = Instant.ofEpochSecond(1_600_000_000L, 123_456L);
    long nanoVal = ValueType.fromTimestampNanos(nanos, ValueMetadata.NULL_METADATA);
    assertEquals(nanos, ValueType.toTimestampNanos(nanoVal, ValueMetadata.NULL_METADATA));
  }

  @Test
  public void testLocalTimestampRoundTrip() {
    LocalDateTime local = LocalDateTime.of(2021, 5, 6, 7, 8, 9, 10_000_000);
    long millis = ValueType.fromLocalTimestampMillis(local, ValueMetadata.NULL_METADATA);
    assertEquals(local, ValueType.toLocalTimestampMillis(millis, ValueMetadata.NULL_METADATA));
    assertEquals(local, ValueType.castToLocalTimestampMillis(local, ValueMetadata.NULL_METADATA));
    assertEquals(local,
        ValueType.castToLocalTimestampMillis(local.toInstant(ZoneOffset.UTC).toEpochMilli(), ValueMetadata.NULL_METADATA));

    LocalDateTime micros = LocalDateTime.of(2021, 5, 6, 7, 8, 9, 11_000);
    long microVal = ValueType.fromLocalTimestampMicros(micros, ValueMetadata.NULL_METADATA);
    assertEquals(micros, ValueType.toLocalTimestampMicros(microVal, ValueMetadata.NULL_METADATA));

    LocalDateTime nanos = LocalDateTime.of(2021, 5, 6, 7, 8, 9, 12_345);
    long nanoVal = ValueType.fromLocalTimestampNanos(nanos, ValueMetadata.NULL_METADATA);
    assertEquals(nanos, ValueType.toLocalTimestampNanos(nanoVal, ValueMetadata.NULL_METADATA));
  }

  @Test
  public void testFromParquetPrimitiveType() {
    assertEquals(ValueType.LONG, valueTypeFor(PrimitiveType.PrimitiveTypeName.INT64));
    assertEquals(ValueType.INT, valueTypeFor(PrimitiveType.PrimitiveTypeName.INT32));
    assertEquals(ValueType.BOOLEAN, valueTypeFor(PrimitiveType.PrimitiveTypeName.BOOLEAN));
    assertEquals(ValueType.BYTES, valueTypeFor(PrimitiveType.PrimitiveTypeName.BINARY));
    assertEquals(ValueType.FLOAT, valueTypeFor(PrimitiveType.PrimitiveTypeName.FLOAT));
    assertEquals(ValueType.DOUBLE, valueTypeFor(PrimitiveType.PrimitiveTypeName.DOUBLE));
  }

  private static ValueType valueTypeFor(PrimitiveType.PrimitiveTypeName name) {
    PrimitiveType type = name == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY
        ? Types.required(name).length(4).named("f")
        : Types.required(name).named("f");
    return ValueType.fromParquetPrimitiveType(type);
  }

  @Test
  public void testFromSchemaPrimitives() {
    assertEquals(ValueType.INT, ValueType.fromSchema(HoodieSchema.create(HoodieSchemaType.INT)));
    assertEquals(ValueType.LONG, ValueType.fromSchema(HoodieSchema.create(HoodieSchemaType.LONG)));
    assertEquals(ValueType.STRING, ValueType.fromSchema(HoodieSchema.create(HoodieSchemaType.STRING)));
    assertEquals(ValueType.BOOLEAN, ValueType.fromSchema(HoodieSchema.create(HoodieSchemaType.BOOLEAN)));
    assertEquals(ValueType.DATE, ValueType.fromSchema(HoodieSchema.create(HoodieSchemaType.DATE)));
    assertEquals(ValueType.UUID, ValueType.fromSchema(HoodieSchema.create(HoodieSchemaType.UUID)));
  }

  @Test
  public void testFromSchemaUnwrapsUnion() {
    HoodieSchema nullableInt = HoodieSchema.createNullable(HoodieSchemaType.INT);
    assertEquals(ValueType.INT, ValueType.fromSchema(nullableInt));
  }

  @Test
  public void testCastToBytesFromFixed() {
    byte[] raw = {9, 8, 7};
    GenericData.Fixed fixed = new GenericData.Fixed(
        org.apache.avro.Schema.createFixed("f", null, null, raw.length), raw);
    assertEquals(ByteBuffer.wrap(raw), ValueType.castToBytes(fixed));
    assertTrue(ValueType.castToBytes(fixed).hasArray());
  }
}
