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

package org.apache.hudi

import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull}
import org.junit.jupiter.api.Test

import java.time.{LocalDateTime, ZoneOffset}

/**
 * Tests that partition values recovered from a partition path are handed back in Catalyst's
 * internal representation, so that they can be held in the [[InternalRow]] of partition values
 * that partition pruning and the file index evaluate against.
 */
class TestHoodieSparkUtilsPartitionValues {

  private val UTC = "UTC"

  @Test
  def testCastStringToTypeReturnsCatalystInternalValues(): Unit = {
    // Date is represented as the number of days since the epoch, not as a string
    assertEquals(DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2023-03-01")),
      HoodieSparkUtils.castStringToType("2023-03-01", DateType, UTC))
    // Timestamp is represented as micros since the epoch, resolved against the given time zone
    val expectedMicros = LocalDateTime.of(2023, 3, 1, 1, 2, 3).toInstant(ZoneOffset.UTC)
      .getEpochSecond * 1000000L
    assertEquals(expectedMicros,
      HoodieSparkUtils.castStringToType("2023-03-01 01:02:03", TimestampType, UTC))
    // ... and the time zone is honored: Asia/Kolkata is a fixed +05:30 off UTC
    assertEquals(expectedMicros - 19800L * 1000000L,
      HoodieSparkUtils.castStringToType("2023-03-01 01:02:03", TimestampType, "Asia/Kolkata"))
    // Decimal is represented as [[Decimal]], not as [[java.math.BigDecimal]]
    assertEquals(Decimal(new java.math.BigDecimal("1.50")),
      HoodieSparkUtils.castStringToType("1.50", DecimalType(10, 2), UTC))

    // Types that were already handled keep working
    assertEquals(5, HoodieSparkUtils.castStringToType("5", IntegerType, UTC))
    assertEquals(5L, HoodieSparkUtils.castStringToType("5", LongType, UTC))
    assertEquals(true, HoodieSparkUtils.castStringToType("true", BooleanType, UTC))
    assertEquals(UTF8String.fromString("abc"),
      HoodieSparkUtils.castStringToType("abc", StringType, UTC))
  }

  @Test
  def testCastStringToTypeMapsDefaultPartitionToNull(): Unit = {
    Seq(StringType, IntegerType, DateType, TimestampType).foreach { dataType =>
      assertNull(HoodieSparkUtils.castStringToType("__HIVE_DEFAULT_PARTITION__", dataType, UTC),
        s"the default partition stands for a null value of $dataType")
    }
  }

  @Test
  def testCastStringToTypeUnescapesValues(): Unit = {
    assertEquals(UTF8String.fromString("a=b"),
      HoodieSparkUtils.castStringToType("a%3Db", StringType, UTC))
  }

  @Test
  def testParsePartitionValuesForSlashSeparatedDatePartitioning(): Unit = {
    // A single date partition column laid out as yyyy/MM/dd does not line up with the partition
    // columns, so it falls back onto [[castStringToType]]
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("grass_date", DateType)))
    val values = HoodieSparkUtils.doParsePartitionColumnValues(
      Array("grass_date"), "2023/03/01", new StoragePath("/tmp/table"), schema, UTC,
      shouldValidatePartitionCols = false, slashSeparatedDatePartitioning = true)

    assertEquals(1, values.length)
    assertEquals(DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2023-03-01")), values.head)
    // The partition row has to be readable through the accessor matching the column's data type
    assertEquals(DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2023-03-01")),
      InternalRow.fromSeq(values.toSeq).getInt(0))
  }

  @Test
  def testParsePartitionValuesForHiveStyleValueHoldingSlash(): Unit = {
    // "san/francisco" spills over into an extra path fragment, so the fragments do not line up
    // with the partition columns and parsing falls back onto [[castStringToType]]
    val schema = StructType(Seq(
      StructField("id", IntegerType), StructField("dt", DateType), StructField("city", StringType)))
    val values = HoodieSparkUtils.doParsePartitionColumnValues(
      Array("dt", "city"), "dt=2023-03-01/city=san/francisco", new StoragePath("/tmp/table"), schema,
      UTC, shouldValidatePartitionCols = false, slashSeparatedDatePartitioning = false)

    assertEquals(2, values.length)
    assertEquals(DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2023-03-01")), values.head)
    assertEquals(UTF8String.fromString("san/francisco"), values(1))
    assertEquals(DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2023-03-01")),
      InternalRow.fromSeq(values.toSeq).getInt(0))
  }
}
