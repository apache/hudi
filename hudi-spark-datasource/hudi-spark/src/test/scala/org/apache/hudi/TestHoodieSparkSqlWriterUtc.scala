/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.common.model.HoodieTimelineTimeZone
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator
import org.apache.hudi.testutils.DataSourceTestUtils
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatterBuilder
import java.time.temporal.ChronoField
import java.util.TimeZone

/**
 * Test suite for SparkSqlWriter class for all cases of using of {@link HoodieTimelineTimeZone.UTC}.
 * Using of {@link HoodieTimelineTimeZone.LOCAL} here could lead to infinite loops, because it could save
 * value of static {@link HoodieInstantTimeGenerator.lastInstantTime} in the heap,
 * which will be greater than instant time for {@link HoodieTimelineTimeZone.UTC}.
 */
class TestHoodieSparkSqlWriterUtc extends HoodieSparkWriterTestBase {
  /*
   * Test case for instant is generated with commit timezone when TIMELINE_TIMEZONE set to UTC
   * related to HUDI-5978
   */
  @Test
  def testInsertDatasetWithTimelineTimezoneUTC(): Unit = {
    val defaultTimezone = TimeZone.getDefault
    try {
      val fooTableModifier = commonTableModifier.updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .updated(DataSourceWriteOptions.INSERT_DROP_DUPS.key, "false")
        .updated(HoodieTableConfig.TIMELINE_TIMEZONE.key, "UTC") // utc timezone

      // generate the inserts
      val schema = DataSourceTestUtils.getStructTypeExampleSchema
      val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
      val records = DataSourceTestUtils.generateRandomRows(100)
      val recordsSeq = convertRowListToSeq(records)
      val df = spark.createDataFrame(sc.parallelize(recordsSeq), structType)

      // get UTC instant before write
      val beforeWriteInstant = Instant.now()

      // set local timezone to America/Los_Angeles(UTC-7)
      TimeZone.setDefault(TimeZone.getTimeZone("Asia/Novosibirsk"))

      // write to Hudi
      val (success, writeInstantTimeOpt, _, _, _, hoodieTableConfig) = HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableModifier, df)
      assertTrue(success)
      val hoodieTableTimelineTimezone = HoodieTimelineTimeZone.valueOf(hoodieTableConfig.getString(HoodieTableConfig.TIMELINE_TIMEZONE))
      assertEquals(hoodieTableTimelineTimezone, HoodieTimelineTimeZone.UTC)

      val utcFormatter = new DateTimeFormatterBuilder()
        .appendPattern(HoodieInstantTimeGenerator.SECS_INSTANT_TIMESTAMP_FORMAT)
        .appendValue(ChronoField.MILLI_OF_SECOND, 3)
        .toFormatter
        .withZone(ZoneId.of("UTC"))
      // instant parsed by UTC timezone
      val writeInstant = Instant.from(utcFormatter.parse(writeInstantTimeOpt.get()))

      assertTrue(beforeWriteInstant.toEpochMilli < writeInstant.toEpochMilli,
        s"writeInstant(${writeInstant.toEpochMilli}) must always be greater than beforeWriteInstant(${beforeWriteInstant.toEpochMilli}) if writeInstant was generated with UTC timezone")
    } finally {
      TimeZone.setDefault(defaultTimezone)
    }
  }
}
