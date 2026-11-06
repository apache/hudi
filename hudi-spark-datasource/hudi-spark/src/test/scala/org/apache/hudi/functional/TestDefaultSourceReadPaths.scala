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

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertTrue}

import scala.collection.JavaConverters._

/**
 * How the datasource reports paths it will not read.
 *
 * Glob paths and [[DataSourceReadOptions.READ_PATHS]] were both rejected in 1.2.0, but by a single
 * condition throwing a single message, so a user passing plain partition paths through read.paths
 * was told their non-glob paths were unsupported glob paths. These cover the three rejections
 * separately, and the reads that must keep working around them.
 */
class TestDefaultSourceReadPaths extends HoodieSparkClientTestBase {

  var spark: SparkSession = null

  private val readPathsKey = DataSourceReadOptions.READ_PATHS.key

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
    FileSystem.closeAll()
    System.gc()
  }

  private def writeTable(tableType: HoodieTableType, path: String, numRecords: Int = 20): Unit = {
    val records = recordsToStrings(dataGen.generateInserts("000", numRecords)).asScala.toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(CommonOptionUtils.commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(path)
  }

  /** Every rejection has to say what to do instead, not just what is refused. */
  private def assertPointsAtReplacement(message: String): Unit = {
    assertTrue(message.contains("Load the table base path"),
      s"the message must point at the supported replacement, got: $message")
  }

  // ---------------------------------------------------------------------------------------------
  // the reads that must keep working
  // ---------------------------------------------------------------------------------------------

  @Test
  def testLoadBasePathStillWorksForCow(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)
    assertEquals(20, spark.read.format("hudi").load(basePath).count())
  }

  @Test
  def testLoadBasePathStillWorksForMor(): Unit = {
    writeTable(HoodieTableType.MERGE_ON_READ, basePath)
    assertEquals(20, spark.read.format("hudi").load(basePath).count())
  }

  /**
   * The replacement the error messages recommend. A predicate on the partition column has to keep
   * returning only that partition, which is what selecting paths by hand used to be for.
   */
  @Test
  def testPartitionPredicateStillPrunes(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath, numRecords = 60)
    val df = spark.read.format("hudi").load(basePath)
    val partitions = df.select("partition").distinct().collect().map(_.getString(0))
    assertTrue(partitions.length > 1, s"need a multi-partition table, got: ${partitions.mkString(",")}")

    val target = partitions.head
    val pruned = df.where(s"partition = '$target'")
    assertTrue(pruned.count() > 0, "the predicate must not filter everything away")
    assertEquals(Seq(target), pruned.select("partition").distinct().collect().map(_.getString(0)).toSeq)
    assertEquals(df.where(s"partition = '$target'").count(), pruned.count())
  }

  /**
   * Reached through Spark's SchemaRelationProvider path, which bypasses the 2-arg overload and so
   * bypasses its HoodieSchemaNotFoundException catch. The guards run here too, and a table with a
   * schema must still read.
   */
  @Test
  def testExplicitSchemaLoadStillWorks(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)
    val schema = spark.read.format("hudi").load(basePath).schema
    assertEquals(20, spark.read.format("hudi").schema(schema).load(basePath).count())
  }

  /** An empty table has no schema on disk; the explicit-schema path must not blow up on it. */
  @Test
  def testExplicitSchemaLoadOnSchemalessTableReturnsEmpty(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)
    val schema = spark.read.format("hudi").load(basePath).schema

    val emptyPath = basePath + "_empty"
    val emptyDF = spark.read.json(spark.sparkContext.emptyRDD[String])
    emptyDF.write.format("hudi")
      .options(CommonOptionUtils.commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(emptyPath)

    assertEquals(0, spark.read.format("hudi").schema(schema).load(emptyPath).count())
  }

  @Test
  def testSparkSqlReadIsUnaffected(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)
    val tableName = "test_default_source_read_paths"
    spark.sql(s"create table $tableName using hudi location '$basePath'")
    try {
      assertEquals(20, spark.sql(s"select * from $tableName").count())
    } finally {
      spark.sql(s"drop table if exists $tableName")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // the three rejections, each reported on its own terms
  // ---------------------------------------------------------------------------------------------

  /**
   * The case from the report: plain partition paths, no wildcard anywhere, previously answered
   * with a complaint about glob paths.
   */
  @Test
  def testReadPathsWithPathReportsReadPathsNotGlob(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)
    val plainPartitionPaths = s"$basePath/2015/03/16,$basePath/2015/03/17"

    val e = assertThrowsHoodie {
      spark.read.format("hudi").option(readPathsKey, plainPartitionPaths).load(basePath).count()
    }
    assertTrue(e.getMessage.contains(readPathsKey), s"must name the option, got: ${e.getMessage}")
    assertFalse(e.getMessage.toLowerCase.contains("glob"),
      s"nothing here is a glob, so the message must not mention one, got: ${e.getMessage}")
    assertPointsAtReplacement(e.getMessage)
  }

  /**
   * read.paths with no path at all. This slipped past the old first guard, since that only fired
   * when both were absent, and died further down instead.
   */
  @Test
  def testReadPathsWithoutPathReportsReadPaths(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)

    val e = assertThrowsHoodie {
      spark.read.format("hudi").option(readPathsKey, s"$basePath/2015/03/16").load().count()
    }
    assertTrue(e.getMessage.contains(readPathsKey), s"must name the option, got: ${e.getMessage}")
    assertFalse(e.getMessage.toLowerCase.contains("glob"), s"not a glob, got: ${e.getMessage}")
    assertPointsAtReplacement(e.getMessage)
  }

  /** Neither option set. Must not advertise read.paths, since setting it throws as well. */
  @Test
  def testNeitherOptionReportsPathOnly(): Unit = {
    val e = assertThrowsHoodie {
      spark.read.format("hudi").load().count()
    }
    assertTrue(e.getMessage.contains("'path' must be specified"), s"got: ${e.getMessage}")
    assertFalse(e.getMessage.contains(readPathsKey),
      s"read.paths also throws, so it must not be offered as the alternative, got: ${e.getMessage}")
    assertPointsAtReplacement(e.getMessage)
  }

  @Test
  def testGlobInPathReportsGlob(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)

    val e = assertThrowsHoodie {
      spark.read.format("hudi").load(s"$basePath/2015/03/*").count()
    }
    assertTrue(e.getMessage.toLowerCase.contains("glob"), s"got: ${e.getMessage}")
    assertFalse(e.getMessage.contains(readPathsKey),
      s"read.paths was not set, so it must not appear, got: ${e.getMessage}")
    assertPointsAtReplacement(e.getMessage)
  }

  /**
   * Both wrong at once. read.paths is reported, because the option is gone outright and its
   * replacement covers the glob case too.
   */
  @Test
  def testReadPathsWinsOverGlobInPath(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)

    val e = assertThrowsHoodie {
      spark.read.format("hudi")
        .option(readPathsKey, s"$basePath/2015/03/16")
        .load(s"$basePath/2015/03/*")
        .count()
    }
    assertTrue(e.getMessage.contains(readPathsKey), s"read.paths must win, got: ${e.getMessage}")
    assertFalse(e.getMessage.toLowerCase.contains("glob"),
      s"the glob message must not win, got: ${e.getMessage}")
  }

  /**
   * An explicitly empty value still counts as setting the option. optParams carries Some(""), and
   * pre-1.2.0 threw on it too, so this keeps the rejection rather than quietly ignoring the key.
   */
  @Test
  def testEmptyReadPathsIsStillRejected(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)

    val e = assertThrowsHoodie {
      spark.read.format("hudi").option(readPathsKey, "").load(basePath).count()
    }
    assertTrue(e.getMessage.contains(readPathsKey), s"got: ${e.getMessage}")
    assertPointsAtReplacement(e.getMessage)
  }

  /** The message the user actually reads has to differ per cause, not just internally. */
  @Test
  def testTheThreeRejectionsDoNotShareAMessage(): Unit = {
    writeTable(HoodieTableType.COPY_ON_WRITE, basePath)

    val readPathsMsg = assertThrowsHoodie {
      spark.read.format("hudi").option(readPathsKey, s"$basePath/2015/03/16").load(basePath).count()
    }.getMessage
    val noPathMsg = assertThrowsHoodie {
      spark.read.format("hudi").load().count()
    }.getMessage
    val globMsg = assertThrowsHoodie {
      spark.read.format("hudi").load(s"$basePath/2015/03/*").count()
    }.getMessage

    assertNotEquals(readPathsMsg, noPathMsg)
    assertNotEquals(readPathsMsg, globMsg)
    assertNotEquals(noPathMsg, globMsg)
  }

  /**
   * Spark wraps datasource failures, so unwrap to the HoodieException the datasource raised rather
   * than asserting on whatever Spark happened to wrap it in.
   */
  private def assertThrowsHoodie(f: => Unit): HoodieException = {
    val thrown = try {
      f
      null
    } catch {
      case t: Throwable => t
    }
    assertTrue(thrown != null, "expected the read to fail, but it succeeded")

    var cause: Throwable = thrown
    while (cause != null && !cause.isInstanceOf[HoodieException]) {
      cause = cause.getCause
    }
    assertTrue(cause != null, s"expected a HoodieException somewhere in the chain, got: $thrown")
    cause.asInstanceOf[HoodieException]
  }
}
