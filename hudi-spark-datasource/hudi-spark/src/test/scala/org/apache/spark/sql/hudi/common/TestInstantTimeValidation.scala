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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.{DataSourceOptionsHelper, DataSourceReadOptions}
import org.apache.hudi.common.table.timeline.HoodieTimeline

import org.apache.spark.sql.hudi.HoodieSqlCommonUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class TestInstantTimeValidation {

  // --- formatQueryInstant: Hudi native formats ---

  @Test
  def testFormatQueryInstantWithHudiMillisFormat(): Unit = {
    val result = HoodieSqlCommonUtils.formatQueryInstant("20250101080000000")
    assertEquals("20250101080000000", result)
  }

  @Test
  def testFormatQueryInstantWithHudiSecsFormat(): Unit = {
    val result = HoodieSqlCommonUtils.formatQueryInstant("20250101080000")
    assertEquals("20250101080000", result)
  }

  // --- formatQueryInstant: ISO date formats ---

  @Test
  def testFormatQueryInstantWithIsoDate(): Unit = {
    val result = HoodieSqlCommonUtils.formatQueryInstant("2025-01-01")
    assertTrue(result.startsWith("20250101"), s"Expected result to start with 20250101 but got: $result")
  }

  @Test
  def testFormatQueryInstantWithIsoDateTimeSpace(): Unit = {
    val result = HoodieSqlCommonUtils.formatQueryInstant("2025-01-02 03:04:56")
    assertTrue(result.startsWith("20250102030456"), s"Expected result to start with 20250102030456 but got: $result")
  }

  @Test
  def testFormatQueryInstantWithIsoDateTimeSpaceMillis(): Unit = {
    val result = HoodieSqlCommonUtils.formatQueryInstant("2025-01-02 03:04:56.789")
    assertEquals("20250102030456789", result)
  }

  @Test
  def testFormatQueryInstantWithIsoDateTimeTSeparator(): Unit = {
    val result = HoodieSqlCommonUtils.formatQueryInstant("2025-01-02T03:04:56")
    assertTrue(result.startsWith("20250102030456"), s"Expected result to start with 20250102030456 but got: $result")
  }

  @Test
  def testFormatQueryInstantWithIsoDateTimeTSeparatorMillis(): Unit = {
    val result = HoodieSqlCommonUtils.formatQueryInstant("2025-01-02T03:04:56.789")
    assertEquals("20250102030456789", result)
  }

  // --- formatQueryInstant: epoch formats ---

  @Test
  def testFormatQueryInstantWithEpochSeconds(): Unit = {
    val result = HoodieSqlCommonUtils.formatQueryInstant("1735689600")
    assertTrue(result.matches("\\d{17}"), s"Expected 17-digit Hudi instant but got: $result")
  }

  @Test
  def testFormatQueryInstantWithEpochMillis(): Unit = {
    val result = HoodieSqlCommonUtils.formatQueryInstant("1735689600000")
    assertTrue(result.matches("\\d{17}"), s"Expected 17-digit Hudi instant but got: $result")
  }

  @Test
  def testFormatQueryInstantEpochSecsAndMillisProduceSameResult(): Unit = {
    val fromSecs = HoodieSqlCommonUtils.formatQueryInstant("1735689600")
    val fromMillis = HoodieSqlCommonUtils.formatQueryInstant("1735689600000")
    assertEquals(fromSecs, fromMillis)
  }

  // --- formatQueryInstant: rejection of invalid inputs ---

  @Test
  def testFormatQueryInstantRejectsShortNumber(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => {
      HoodieSqlCommonUtils.formatQueryInstant("42")
    })
  }

  @Test
  def testFormatQueryInstantRejectsRandomString(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => {
      HoodieSqlCommonUtils.formatQueryInstant("abc")
    })
  }

  @Test
  def testFormatQueryInstantRejectsNegativeNumber(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => {
      HoodieSqlCommonUtils.formatQueryInstant("-1")
    })
  }

  @Test
  def testFormatQueryInstantRejectsFiveDigitNumber(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => {
      HoodieSqlCommonUtils.formatQueryInstant("12345")
    })
  }

  // --- formatIncrementalInstant: sentinel pass-through ---

  @Test
  def testFormatIncrementalInstantPassesThroughEarliest(): Unit = {
    assertEquals("earliest", HoodieSqlCommonUtils.formatIncrementalInstant("earliest"))
  }

  @Test
  def testFormatIncrementalInstantPassesThroughZeroSentinel(): Unit = {
    assertEquals("000", HoodieSqlCommonUtils.formatIncrementalInstant("000"))
  }

  @Test
  def testFormatIncrementalInstantPassesThroughLegacyZeroPrefixedInstants(): Unit = {
    assertEquals("0", HoodieSqlCommonUtils.formatIncrementalInstant("0"))
    assertEquals("001", HoodieSqlCommonUtils.formatIncrementalInstant("001"))
    assertEquals("0000", HoodieSqlCommonUtils.formatIncrementalInstant("0000"))
  }

  @Test
  def testFormatIncrementalInstantPassesThroughBootstrapInstants(): Unit = {
    assertEquals(HoodieTimeline.INIT_INSTANT_TS,
      HoodieSqlCommonUtils.formatIncrementalInstant(HoodieTimeline.INIT_INSTANT_TS))
    assertEquals(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS,
      HoodieSqlCommonUtils.formatIncrementalInstant(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS))
    assertEquals(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
      HoodieSqlCommonUtils.formatIncrementalInstant(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS))
  }

  // --- formatIncrementalInstant: normalization ---

  @Test
  def testFormatIncrementalInstantNormalizesHudiFormat(): Unit = {
    assertEquals("20250901080000", HoodieSqlCommonUtils.formatIncrementalInstant("20250901080000"))
  }

  @Test
  def testFormatIncrementalInstantNormalizesIsoDate(): Unit = {
    val result = HoodieSqlCommonUtils.formatIncrementalInstant("2025-01-02")
    assertTrue(result.startsWith("20250102"), s"Expected result to start with 20250102 but got: $result")
  }

  @Test
  def testFormatIncrementalInstantNormalizesEpochSeconds(): Unit = {
    val result = HoodieSqlCommonUtils.formatIncrementalInstant("1735689600")
    assertTrue(result.matches("\\d{17}"), s"Expected 17-digit Hudi instant but got: $result")
  }

  @Test
  def testFormatIncrementalInstantNormalizesEpochMillis(): Unit = {
    val result = HoodieSqlCommonUtils.formatIncrementalInstant("1735689600000")
    assertTrue(result.matches("\\d{17}"), s"Expected 17-digit Hudi instant but got: $result")
  }

  // --- formatIncrementalInstant: rejection ---

  @Test
  def testFormatIncrementalInstantRejectsShortNumber(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => {
      HoodieSqlCommonUtils.formatIncrementalInstant("42")
    })
  }

  @Test
  def testFormatIncrementalInstantRejectsRandomString(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => {
      HoodieSqlCommonUtils.formatIncrementalInstant("not_a_timestamp")
    })
  }

  // --- parametersWithReadDefaults: incremental timestamp normalization ---

  @Test
  def testReadDefaultsNormalizesStartCommitIsoDate(): Unit = {
    val params = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      DataSourceReadOptions.START_COMMIT.key -> "2025-01-02"
    ))
    val result = params(DataSourceReadOptions.START_COMMIT.key)
    assertTrue(result.startsWith("20250102"), s"Expected normalized instant starting with 20250102, got: $result")
  }

  @Test
  def testReadDefaultsNormalizesEndCommitIsoDateTimeT(): Unit = {
    val params = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      DataSourceReadOptions.END_COMMIT.key -> "2025-06-15T10:30:00.123"
    ))
    assertEquals("20250615103000123", params(DataSourceReadOptions.END_COMMIT.key))
  }

  @Test
  def testReadDefaultsAllowsEarliestSentinel(): Unit = {
    val params = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      DataSourceReadOptions.START_COMMIT.key -> "earliest"
    ))
    assertEquals("earliest", params(DataSourceReadOptions.START_COMMIT.key))
  }

  @Test
  def testReadDefaultsAllowsLegacyZeroPrefixedInstants(): Unit = {
    val params = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      DataSourceReadOptions.START_COMMIT.key -> "0",
      DataSourceReadOptions.END_COMMIT.key -> "001"
    ))
    assertEquals("0", params(DataSourceReadOptions.START_COMMIT.key))
    assertEquals("001", params(DataSourceReadOptions.END_COMMIT.key))
  }

  @Test
  def testReadDefaultsRejectsInvalidStartCommit(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => {
      DataSourceOptionsHelper.parametersWithReadDefaults(Map(
        DataSourceReadOptions.START_COMMIT.key -> "42"
      ))
    })
  }

  @Test
  def testReadDefaultsRejectsInvalidEndCommit(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => {
      DataSourceOptionsHelper.parametersWithReadDefaults(Map(
        DataSourceReadOptions.END_COMMIT.key -> "abc"
      ))
    })
  }

  @Test
  def testReadDefaultsNormalizesEpochSeconds(): Unit = {
    val params = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      DataSourceReadOptions.START_COMMIT.key -> "1735689600"
    ))
    val result = params(DataSourceReadOptions.START_COMMIT.key)
    assertTrue(result.matches("\\d{17}"), s"Expected 17-digit Hudi instant but got: $result")
  }

  @Test
  def testReadDefaultsPassesThroughValidHudiInstant(): Unit = {
    val params = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      DataSourceReadOptions.START_COMMIT.key -> "20250901080000000"
    ))
    assertEquals("20250901080000000", params(DataSourceReadOptions.START_COMMIT.key))
  }

  @Test
  def testReadDefaultsDoesNotAffectMissingTimestamps(): Unit = {
    val params = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      "hoodie.datasource.query.type" -> "incremental"
    ))
    assertTrue(!params.contains(DataSourceReadOptions.START_COMMIT.key))
    assertTrue(!params.contains(DataSourceReadOptions.END_COMMIT.key))
  }
}
