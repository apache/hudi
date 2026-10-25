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

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertTrue}

import java.util.function.Consumer

/**
 * User-set rebase mode and session timezone must reach Spark executor tasks
 * dispatched outside a SQL execution context. Each test fails without the
 * SparkEnv.get.conf fallback.
 */
class TestSparkAdapterRebaseModePropagation extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  override def getSparkSessionExtensionsInjector: HOption[Consumer[SparkSessionExtensions]] =
    toJavaOption(Some(JFunction.toJavaConsumer((rcv: SparkSessionExtensions) =>
      new HoodieSparkSessionExtension().apply(rcv))))

  // Use a non-default timezone so we can prove that the user override is what
  // the executor sees, not the JVM default that any fresh SQLConf would return.
  // We pick whichever of two unrelated zones isn't the JVM default, so the test
  // works on any developer's machine (or CI box).
  private val customTimeZone: String = {
    val jvmDefault = java.util.TimeZone.getDefault.getID
    if (jvmDefault == "Asia/Tokyo") "Pacific/Auckland" else "Asia/Tokyo"
  }

  @BeforeEach override def setUp(): Unit = {
    // Customer-style config: LEGACY rebase + a non-default session timezone in
    // SparkConf (broadcast to all executors) AND propagated into the driver
    // SparkSession's SQLConf at startup.
    extraConf.put("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    extraConf.put("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    extraConf.put("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    extraConf.put("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
    extraConf.put("spark.sql.session.timeZone", customTimeZone)

    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupFileSystem()
  }

  @Test
  def testRebaseModeReachesExecutorTask(): Unit = {
    // Sanity check #1: SparkConf carries LEGACY (so SparkEnv.get.conf
    // on the executor will see it after the fix).
    assertEquals(
      "LEGACY",
      spark.sparkContext.getConf.get("spark.sql.parquet.datetimeRebaseModeInWrite"),
      "SparkConf does not carry LEGACY — extraConf setup failed"
    )

    // Sanity check #2: Driver session's SQLConf has LEGACY.
    assertEquals(
      "LEGACY",
      spark.conf.get("spark.sql.parquet.datetimeRebaseModeInWrite"),
      "driver SparkSession SQLConf does not see LEGACY"
    )

    // Sanity check #3: The Hudi adapter, called from the driver, returns LEGACY.
    val driverMode = SparkAdapterSupport.sparkAdapter.getDateTimeRebaseMode().toString
    assertEquals(
      "LEGACY", driverMode,
      s"adapter on driver returned $driverMode — expected LEGACY"
    )

    // Now the actual bug check: ask the adapter on EXECUTOR task threads.
    // We use raw RDD.map (NOT Dataset.map) so we are NOT inside a Spark SQL
    // execution context — Spark will not auto-propagate SQLConf for us.
    // This mirrors HoodieCompactor.compact:146's `context.parallelize(...).map(...)`.
    val seenOnExecutor: Array[String] = spark.sparkContext.parallelize(1 to 4, 4).map { _ =>
      SparkAdapterSupport.sparkAdapter.getDateTimeRebaseMode().toString
    }.collect()

    seenOnExecutor.zipWithIndex.foreach { case (mode, i) =>
      assertEquals(
        "LEGACY", mode,
        s"executor task #$i resolved rebase mode to '$mode' despite the driver " +
          "session being LEGACY and SparkConf carrying LEGACY. This is the " +
          "SQLConf-not-propagated-to-executor bug. " +
          "Fix: have the adapter's getDateTimeRebaseMode() fall back to " +
          "SparkEnv.get.conf when SQLConf.get does not have the override."
      )
    }
  }

  /** SparkConf must be reachable on executor task threads via SparkEnv.get.conf. */
  @Test
  def testSparkConfIsVisibleOnExecutorViaSparkEnv(): Unit = {
    val key = "spark.sql.parquet.datetimeRebaseModeInWrite"
    val seen: Array[String] = spark.sparkContext.parallelize(1 to 4, 4).map { _ =>
      Option(SparkEnv.get).map(_.conf.get(key, "<absent>")).getOrElse("<no SparkEnv>")
    }.collect()
    seen.zipWithIndex.foreach { case (v, i) =>
      assertEquals(
        "LEGACY", v,
        s"task #$i could not read $key from SparkEnv.get.conf; got '$v'. " +
          "If this fails, the adapter SparkEnv fallback path is unreachable on this Spark version."
      )
    }
  }

  /** Same shape as the rebase-mode test, for `HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone`. */
  @Test
  def testSessionLocalTimeZoneReachesExecutorTask(): Unit = {
    // Sanity: customTimeZone is set on the driver session.
    assertEquals(customTimeZone, spark.conf.get("spark.sql.session.timeZone"))
    assertNotEquals(customTimeZone, java.util.TimeZone.getDefault.getID,
      "test setup is fragile: customTimeZone matches the JVM default, so we " +
        "cannot distinguish 'fix worked' from 'fell back to JVM default'")

    // Sanity: helper returns the override when called on the driver thread.
    assertEquals(customTimeZone, HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone())

    // Bug check: helper returns the override when called from an executor task,
    // not the JVM default.
    val seen: Array[String] = spark.sparkContext.parallelize(1 to 4, 4).map { _ =>
      HoodieRowParquetWriteSupport.resolveSessionLocalTimeZone()
    }.collect()
    seen.zipWithIndex.foreach { case (tz, i) =>
      assertEquals(
        customTimeZone, tz,
        s"executor task #$i resolved sessionLocalTimeZone to '$tz', not the " +
          s"user override '$customTimeZone'. Same SQLConf-not-propagated bug " +
          "as the rebase mode read. Fix: use SparkEnv.get.conf as a fallback."
      )
    }
  }
}
