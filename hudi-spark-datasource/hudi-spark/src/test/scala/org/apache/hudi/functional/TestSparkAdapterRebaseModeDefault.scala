/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */

package org.apache.hudi.functional

import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.io.storage.row.HoodieRowParquetWriteSupport
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.internal.SQLConf
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.assertEquals

import java.util.function.Consumer

/**
 * Regression: with no override on SparkSession or SparkConf, the resolution
 * chain falls through to the documented defaults (driver and executor).
 */
class TestSparkAdapterRebaseModeDefault extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  override def getSparkSessionExtensionsInjector: HOption[Consumer[SparkSessionExtensions]] =
    toJavaOption(Some(JFunction.toJavaConsumer((rcv: SparkSessionExtensions) =>
      new HoodieSparkSessionExtension().apply(rcv))))

  @BeforeEach override def setUp(): Unit = {
    // Deliberately set no rebase or timezone overrides in extraConf; the
    // session is created with whatever defaults Spark ships with.
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupFileSystem()
  }

  /**
   * Adapter falls through to the ConfigEntry default. The default differs by
   * Spark version (EXCEPTION on 3.x, CORRECTED on 4.1+), so we read it
   * dynamically from SQLConf.
   */
  @Test
  def testRebaseModeDefaultsToConfigEntryDefault(): Unit = {
    val expected = SQLConf.get.getConf(SQLConf.PARQUET_REBASE_MODE_IN_WRITE).toString

    assertEquals(expected,
      SparkAdapterSupport.sparkAdapter.getDateTimeRebaseMode().toString,
      "driver-side adapter did not return the ConfigEntry default")

    val seenOnExecutor: Array[String] = spark.sparkContext.parallelize(1 to 4, 4).map { _ =>
      SparkAdapterSupport.sparkAdapter.getDateTimeRebaseMode().toString
    }.collect()

    seenOnExecutor.zipWithIndex.foreach { case (mode, i) =>
      assertEquals(expected, mode,
        s"executor task #$i resolved rebase mode to '$mode' with no overrides set; " +
          s"expected the ConfigEntry default ('$expected').")
    }
  }

  /** Timezone helper falls through to SQLConf's session-local timezone (JVM default). */
  @Test
  def testSessionLocalTimeZoneDefaultsToJvmDefault(): Unit = {
    val expected = java.util.TimeZone.getDefault.getID

    assertEquals(expected,
      HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone(),
      "driver-side helper did not return the JVM default timezone")

    val seen: Array[String] = spark.sparkContext.parallelize(1 to 4, 4).map { _ =>
      HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone()
    }.collect()

    seen.zipWithIndex.foreach { case (tz, i) =>
      assertEquals(expected, tz,
        s"executor task #$i resolved sessionLocalTimeZone to '$tz' with no overrides; " +
          s"expected the JVM default ('$expected').")
    }
  }
}
