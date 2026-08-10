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

import org.apache.hudi.metadata.HoodieIndexVersion;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit coverage for the Spark value-metadata conversion helpers. Exercises the
 * Spark-type to {@link ValueType} matrix, decimal precision/scale carry-through,
 * and the value conversion round-trips using only in-process objects.
 */
class TestSparkValueMetadataUtils {

  private static ValueMetadata metadataFor(DataType dataType) {
    return SparkValueMetadataUtils.getValueMetadata(dataType, HoodieIndexVersion.V2);
  }

  private static Stream<Arguments> dataTypeToValueType() {
    return Stream.of(
        Arguments.of(DataTypes.BooleanType, ValueType.BOOLEAN),
        Arguments.of(DataTypes.IntegerType, ValueType.INT),
        Arguments.of(DataTypes.ShortType, ValueType.INT),
        Arguments.of(DataTypes.ByteType, ValueType.INT),
        Arguments.of(DataTypes.LongType, ValueType.LONG),
        Arguments.of(DataTypes.FloatType, ValueType.FLOAT),
        Arguments.of(DataTypes.DoubleType, ValueType.DOUBLE),
        Arguments.of(DataTypes.StringType, ValueType.STRING),
        Arguments.of(DataTypes.TimestampType, ValueType.TIMESTAMP_MICROS),
        Arguments.of(DataTypes.DateType, ValueType.DATE),
        Arguments.of(DataTypes.BinaryType, ValueType.BYTES),
        Arguments.of(DataTypes.NullType, ValueType.NULL));
  }

  @ParameterizedTest
  @MethodSource("dataTypeToValueType")
  void getValueMetadataMapsSparkTypeToValueType(DataType dataType, ValueType expected) {
    ValueMetadata metadata = SparkValueMetadataUtils.getValueMetadata(dataType, HoodieIndexVersion.V2);
    assertEquals(expected, metadata.getValueType(),
        "Spark type " + dataType.typeName() + " must map to value type " + expected);
  }

  @Test
  void getValueMetadataCarriesDecimalPrecisionAndScale() {
    DecimalType decimalType = new DecimalType(12, 4);
    ValueMetadata metadata = SparkValueMetadataUtils.getValueMetadata(decimalType, HoodieIndexVersion.V2);

    assertEquals(ValueType.DECIMAL, metadata.getValueType());
    assertEquals("12,4", metadata.getValueTypeInfo().getAdditionalInfo(),
        "precision and scale must be encoded in the value type info");
  }

  @Test
  void getValueMetadataReturnsV1EmptyBelowV2() {
    // Any index version lower than V2 is unversioned column stats and yields the shared empty metadata.
    ValueMetadata metadata = SparkValueMetadataUtils.getValueMetadata(DataTypes.IntegerType, HoodieIndexVersion.V1);
    assertSame(ValueMetadata.V1EmptyMetadata.get(), metadata,
        "index versions below V2 must return the shared V1 empty metadata");
    assertTrue(metadata.isV1());
  }

  @Test
  void getValueMetadataReturnsNullMetadataForNullType() {
    // A null Spark data type is distinct from NullType and maps to the shared NULL metadata singleton.
    ValueMetadata metadata = SparkValueMetadataUtils.getValueMetadata(null, HoodieIndexVersion.V2);
    assertSame(ValueMetadata.NULL_METADATA, metadata);
    assertEquals(ValueType.NULL, metadata.getValueType());
  }

  @Test
  void convertSparkToJavaReturnsNullForNullInput() {
    ValueMetadata metadata = metadataFor(DataTypes.IntegerType);
    assertNull(SparkValueMetadataUtils.convertSparkToJava(metadata, null));
  }

  @Test
  void convertSparkToJavaPassesThroughPrimitives() {
    assertEquals(Boolean.TRUE,
        SparkValueMetadataUtils.convertSparkToJava(metadataFor(DataTypes.BooleanType), true));
    assertEquals(42,
        SparkValueMetadataUtils.convertSparkToJava(metadataFor(DataTypes.IntegerType), 42));
    assertEquals(42L,
        SparkValueMetadataUtils.convertSparkToJava(metadataFor(DataTypes.LongType), 42L));
    assertEquals(1.5f,
        SparkValueMetadataUtils.convertSparkToJava(metadataFor(DataTypes.FloatType), 1.5f));
    assertEquals(2.5d,
        SparkValueMetadataUtils.convertSparkToJava(metadataFor(DataTypes.DoubleType), 2.5d));
    assertEquals("hudi",
        SparkValueMetadataUtils.convertSparkToJava(metadataFor(DataTypes.StringType), "hudi"));
  }

  @Test
  void convertSparkToJavaHandlesDecimal() {
    ValueMetadata metadata = metadataFor(new DecimalType(10, 2));
    Decimal sparkDecimal = Decimal.apply(new BigDecimal("123.45"));
    Comparable<?> result = SparkValueMetadataUtils.convertSparkToJava(metadata, sparkDecimal);
    assertEquals(new BigDecimal("123.45"), result, "decimal must convert to a java BigDecimal of equal value");
  }

  @Test
  void convertSparkToJavaHandlesBytes() {
    ValueMetadata metadata = metadataFor(DataTypes.BinaryType);
    byte[] input = new byte[] {1, 2, 3, 4};
    Comparable<?> result = SparkValueMetadataUtils.convertSparkToJava(metadata, input);
    assertTrue(result instanceof ByteBuffer, "bytes must convert to a ByteBuffer");
    ByteBuffer buffer = (ByteBuffer) result;
    byte[] roundTripped = new byte[buffer.remaining()];
    buffer.get(roundTripped);
    assertArrayEquals(input, roundTripped,
        "byte content must survive the conversion");
  }

  @Test
  void convertSparkToJavaHandlesDateFromEpochDays() {
    ValueMetadata metadata = metadataFor(DataTypes.DateType);
    // Spark stores dates internally as epoch-day integers.
    int epochDays = (int) LocalDate.of(2021, 3, 15).toEpochDay();
    Comparable<?> result = SparkValueMetadataUtils.convertSparkToJava(metadata, epochDays);
    assertEquals(LocalDate.of(2021, 3, 15), result, "epoch-day int must convert to the matching LocalDate");
  }

  @Test
  void convertSparkToJavaHandlesTimestampMicros() {
    ValueMetadata metadata = metadataFor(DataTypes.TimestampType);
    Instant expected = Instant.ofEpochSecond(1_600_000_000L);
    long micros = expected.getEpochSecond() * 1_000_000L;
    Comparable<?> result = SparkValueMetadataUtils.convertSparkToJava(metadata, micros);
    assertEquals(expected, result, "micros-since-epoch must convert to the matching Instant");
  }

  @Test
  void convertJavaTypeToSparkTypeConvertsInstantWhenLegacyApi() {
    Instant instant = Instant.ofEpochSecond(1_600_000_000L);
    // With the java8 API disabled Spark expects java.sql.Timestamp for timestamp values.
    Object legacy = SparkValueMetadataUtils.convertJavaTypeToSparkType(instant, false);
    assertEquals(Timestamp.from(instant), legacy);

    // With the java8 API enabled the Instant is passed through untouched.
    assertSame(instant, SparkValueMetadataUtils.convertJavaTypeToSparkType(instant, true));
  }

  @Test
  void convertJavaTypeToSparkTypeConvertsLocalDateWhenLegacyApi() {
    LocalDate date = LocalDate.of(2022, 6, 1);
    Object legacy = SparkValueMetadataUtils.convertJavaTypeToSparkType(date, false);
    assertEquals(Date.valueOf(date), legacy);

    assertSame(date, SparkValueMetadataUtils.convertJavaTypeToSparkType(date, true));
  }

  @Test
  void convertJavaTypeToSparkTypeLeavesOtherTypesUntouched() {
    assertSame("plain", SparkValueMetadataUtils.convertJavaTypeToSparkType("plain", false));
    Integer value = 7;
    assertSame(value, SparkValueMetadataUtils.convertJavaTypeToSparkType(value, false));
  }
}
