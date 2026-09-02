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

package org.apache.hudi.dataframe

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.hadoop.fs.HadoopFSUtils

import org.apache.spark.sql.{functions, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterAll, Assertions, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.io.TempDir

import java.nio.file.Path

import scala.collection.JavaConverters._

/**
 * End-to-end tests of the DataFrame write path through the Spark datasource:
 * insert then multiple upsert commits, read back with the regular Hudi datasource.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestHoodieDataFrameWriter {

  @TempDir
  var tempDir: Path = _

  private var spark: SparkSession = _

  private val schema = StructType(Seq(
    StructField("key", StringType, nullable = false),
    StructField("partition", StringType, nullable = false),
    StructField("ts", LongType, nullable = false),
    StructField("value", StringType, nullable = false)))

  @BeforeAll
  def setUp(): Unit = {
    spark = SparkSession.builder()
      .master("local[2]")
      .appName("hoodie-dataframe-write-path-test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  @AfterAll
  def tearDown(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  private def writeOptions(operation: String,
                           extraOpts: Map[String, String] = Map.empty): Map[String, String] = Map(
    "hoodie.table.name" -> "test_dataframe_table",
    HoodieDataFrameWriter.DATAFRAME_WRITE_PATH_ENABLE -> "true",
    HoodieDataFrameWriter.RECORD_KEY_FIELD -> "key",
    HoodieDataFrameWriter.PARTITION_PATH_FIELD -> "partition",
    "hoodie.datasource.write.precombine.field" -> "ts",
    "hoodie.embed.timeline.server" -> "false",
    HoodieDataFrameWriter.OPERATION_KEY -> operation) ++ extraOpts

  private def makeDf(rows: Seq[(String, String, Long, String)]) = {
    spark.createDataFrame(
      rows.map(r => Row(r._1, r._2, r._3, r._4)).asJava, schema)
  }

  private def writeHudi(rows: Seq[(String, String, Long, String)],
                        operation: String,
                        path: String,
                        extraOpts: Map[String, String] = Map.empty): Unit = {
    makeDf(rows).write.format("hudi")
      .options(writeOptions(operation, extraOpts))
      .mode(SaveMode.Append)
      .save(path)
  }

  private def readAsMap(path: String): Map[String, (Long, String, String)] = {
    spark.read.format("hudi").load(path)
      .select("key", "ts", "value", "_hoodie_record_key")
      .collect()
      .map(r => r.getString(0) -> (r.getLong(1), r.getString(2), r.getString(3)))
      .toMap
  }

  private def completedCommits(path: String): Int = {
    HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .setBasePath(path)
      .build()
      .getActiveTimeline.getCommitsTimeline.filterCompletedInstants.countInstants()
  }

  @Test
  def testInsertAndMultipleUpsertsEndToEnd(): Unit = {
    val path = tempDir.resolve("test_table").toString

    // Commit 1: insert into two partitions.
    writeHudi(Seq(
      ("k1", "p1", 1L, "v1"),
      ("k2", "p1", 1L, "v2"),
      ("k3", "p2", 1L, "v3"),
      ("k4", "p2", 1L, "v4")), "insert", path)

    var result = readAsMap(path)
    Assertions.assertEquals(4, result.size)
    Assertions.assertEquals((1L, "v1", "k1"), result("k1"))
    Assertions.assertEquals((1L, "v3", "k3"), result("k3"))
    Assertions.assertEquals(1, completedCommits(path))

    // Commit 2: upsert updating existing keys, deduping within the batch by ordering value,
    // and inserting a new key.
    writeHudi(Seq(
      ("k2", "p1", 2L, "v2-updated"),
      ("k3", "p2", 5L, "v3-stale"),
      ("k3", "p2", 10L, "v3-updated"),
      ("k5", "p2", 2L, "v5")), "upsert", path)

    result = readAsMap(path)
    Assertions.assertEquals(5, result.size)
    Assertions.assertEquals((1L, "v1", "k1"), result("k1"))
    Assertions.assertEquals((2L, "v2-updated", "k2"), result("k2"))
    Assertions.assertEquals((10L, "v3-updated", "k3"), result("k3"))
    Assertions.assertEquals((1L, "v4", "k4"), result("k4"))
    Assertions.assertEquals((2L, "v5", "k5"), result("k5"))
    Assertions.assertEquals(2, completedCommits(path))

    // Commit 3: another upsert on top, updating the previously inserted key.
    writeHudi(Seq(
      ("k5", "p2", 3L, "v5-updated"),
      ("k1", "p1", 4L, "v1-updated")), "upsert", path)

    result = readAsMap(path)
    Assertions.assertEquals(5, result.size)
    Assertions.assertEquals((4L, "v1-updated", "k1"), result("k1"))
    Assertions.assertEquals((3L, "v5-updated", "k5"), result("k5"))
    Assertions.assertEquals((10L, "v3-updated", "k3"), result("k3"))
    Assertions.assertEquals(3, completedCommits(path))

    // Commit 4: read-modify-write round trip; the input carries Hudi meta columns which the
    // writer must drop and regenerate.
    spark.read.format("hudi").load(path)
      .filter(functions.col("key") === "k1")
      .withColumn("ts", functions.lit(100L))
      .withColumn("value", functions.lit("v1-roundtrip"))
      .write.format("hudi")
      .options(writeOptions("upsert"))
      .mode(SaveMode.Append)
      .save(path)

    result = readAsMap(path)
    Assertions.assertEquals(5, result.size)
    Assertions.assertEquals((100L, "v1-roundtrip", "k1"), result("k1"))
    Assertions.assertEquals((3L, "v5-updated", "k5"), result("k5"))
    Assertions.assertEquals(4, completedCommits(path))

    // No duplicate record keys across the table after the merges.
    val keys = spark.read.format("hudi").load(path).select("_hoodie_record_key").collect().map(_.getString(0))
    Assertions.assertEquals(keys.length, keys.distinct.length)
  }

  @Test
  def testInsertsPadExistingSmallFiles(): Unit = {
    val path = tempDir.resolve("test_table_padding").toString

    writeHudi(Seq(("s1", "p1", 1L, "x1"), ("s2", "p1", 1L, "x2")), "insert", path)
    writeHudi(Seq(("s3", "p1", 1L, "x3"), ("s4", "p1", 1L, "x4")), "insert", path)

    val snapshot = spark.read.format("hudi").load(path)
    Assertions.assertEquals(4, snapshot.count())
    // The second insert pads the small file group from the first commit instead of opening a
    // new one.
    val fileIds = snapshot.select("_hoodie_file_name").collect()
      .map(r => org.apache.hudi.common.fs.FSUtils.getFileId(r.getString(0))).distinct
    Assertions.assertEquals(1, fileIds.length)
    Assertions.assertEquals(2, completedCommits(path))
  }

  @Test
  def testUpsertWithRecordLevelIndexTagging(): Unit = {
    val path = tempDir.resolve("test_table_rli").toString
    val rliOpts = Map(
      "hoodie.metadata.record.index.enable" -> "true",
      "hoodie.index.type" -> "RECORD_INDEX")

    writeHudi(Seq(
      ("r1", "p1", 1L, "w1"),
      ("r2", "p1", 1L, "w2"),
      ("r3", "p2", 1L, "w3")), "insert", path, rliOpts)

    // The record index partition exists after the first commit, so the second write tags
    // through the point-lookup strategy.
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .setBasePath(path)
      .build()
    Assertions.assertTrue(metaClient.getTableConfig.isMetadataPartitionAvailable(
      org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX))

    writeHudi(Seq(
      ("r2", "p1", 5L, "w2-updated"),
      ("r4", "p2", 2L, "w4")), "upsert", path, rliOpts)

    val result = readAsMap(path)
    Assertions.assertEquals(4, result.size)
    Assertions.assertEquals((1L, "w1", "r1"), result("r1"))
    Assertions.assertEquals((5L, "w2-updated", "r2"), result("r2"))
    Assertions.assertEquals((1L, "w3", "r3"), result("r3"))
    Assertions.assertEquals((2L, "w4", "r4"), result("r4"))
    Assertions.assertEquals(2, completedCommits(path))

    val keys = spark.read.format("hudi").load(path).select("_hoodie_record_key").collect().map(_.getString(0))
    Assertions.assertEquals(keys.length, keys.distinct.length)
  }

  @Test
  def testUpsertIntoNewTableBehavesAsInsert(): Unit = {
    val path = tempDir.resolve("test_table_upsert_first").toString
    writeHudi(Seq(("a1", "p1", 1L, "x1"), ("a2", "p2", 1L, "x2")), "upsert", path)

    val result = readAsMap(path)
    Assertions.assertEquals(2, result.size)
    Assertions.assertEquals((1L, "x1", "a1"), result("a1"))
    Assertions.assertEquals(1, completedCommits(path))
  }
}
