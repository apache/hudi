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
    // A stale hudi-spark-common on the classpath (e.g. reinstalled from another branch) lacks
    // the datasource gate, and every write below would silently exercise the legacy path
    // instead of the DataFrame write path under test.
    Assertions.assertTrue(
      classOf[org.apache.hudi.DefaultSource].getDeclaredMethods.exists(_.getName.contains("runDataFrameWritePath")),
      "DefaultSource on the test classpath lacks the DataFrame write path gate; "
        + "rebuild hudi-spark-common from this branch")
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
  def testShuffleStatsProfilerEndToEnd(): Unit = {
    val path = tempDir.resolve("test_table_shuffle_stats").toString
    val extra = Map(
      HoodieDataFrameWriter.PROFILER -> HoodieDataFrameWriter.PROFILER_SHUFFLE_STATS,
      HoodieDataFrameWriter.SHUFFLE_UPDATE_REDUCERS -> "4",
      HoodieDataFrameWriter.SHUFFLE_INSERT_REDUCERS -> "4")

    writeHudi(Seq(
      ("h1", "p1", 1L, "y1"),
      ("h2", "p1", 1L, "y2"),
      ("h3", "p2", 1L, "y3"),
      ("h4", "p2", 1L, "y4")), "insert", path, extra)
    writeHudi(Seq(
      ("h2", "p1", 3L, "y2-updated"),
      ("h4", "p2", 3L, "y4-updated"),
      ("h5", "p2", 2L, "y5")), "upsert", path, extra)

    val result = readAsMap(path)
    Assertions.assertEquals(5, result.size)
    Assertions.assertEquals((1L, "y1", "h1"), result("h1"))
    Assertions.assertEquals((3L, "y2-updated", "h2"), result("h2"))
    Assertions.assertEquals((1L, "y3", "h3"), result("h3"))
    Assertions.assertEquals((3L, "y4-updated", "h4"), result("h4"))
    Assertions.assertEquals((2L, "y5", "h5"), result("h5"))
    Assertions.assertEquals(2, completedCommits(path))

    // The shuffle bytes land in commit metadata so later writes can calibrate bin sizes.
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .setBasePath(path)
      .build()
    val lastInstant = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()
    val commitMetadata = metaClient.getActiveTimeline.readCommitMetadata(lastInstant)
    Assertions.assertTrue(
      commitMetadata.getExtraMetadata.containsKey(HoodieDataFrameWriter.SHUFFLE_BYTES_METADATA_KEY))
    Assertions.assertTrue(
      commitMetadata.getExtraMetadata.get(HoodieDataFrameWriter.SHUFFLE_BYTES_METADATA_KEY).toLong > 0)

    val keys = spark.read.format("hudi").load(path).select("_hoodie_record_key").collect().map(_.getString(0))
    Assertions.assertEquals(keys.length, keys.distinct.length)
  }

  @Test
  def testComplexKeysWithHiveStylePartitioning(): Unit = {
    val path = tempDir.resolve("test_table_complex").toString
    val opts = writeOptions("insert", Map(
      HoodieDataFrameWriter.RECORD_KEY_FIELD -> "key,partition",
      "hoodie.datasource.write.hive_style_partitioning" -> "true"))
    makeDf(Seq(("c1", "p1", 1L, "z1"), ("c2", "p2", 1L, "z2"))).write.format("hudi")
      .options(opts).mode(SaveMode.Append).save(path)
    makeDf(Seq(("c1", "p1", 5L, "z1-updated"))).write.format("hudi")
      .options(opts + (HoodieDataFrameWriter.OPERATION_KEY -> "upsert")).mode(SaveMode.Append).save(path)

    val rows = spark.read.format("hudi").load(path)
      .select("_hoodie_record_key", "_hoodie_partition_path", "value").collect()
      .map(r => (r.getString(0), r.getString(1), r.getString(2))).toSet
    Assertions.assertEquals(Set(
      ("key:c1,partition:p1", "partition=p1", "z1-updated"),
      ("key:c2,partition:p2", "partition=p2", "z2")), rows)
  }

  @Test
  def testTimestampKeyGeneratorPartitioning(): Unit = {
    val path = tempDir.resolve("test_table_timestamp").toString
    val opts = writeOptions("insert", Map(
      "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
      HoodieDataFrameWriter.PARTITION_PATH_FIELD -> "ts",
      "hoodie.keygen.timebased.timestamp.type" -> "UNIX_TIMESTAMP",
      "hoodie.keygen.timebased.output.dateformat" -> "yyyyMMdd"))
    makeDf(Seq(("t1", "unused", 0L, "a"), ("t2", "unused", 86400L, "b"))).write.format("hudi")
      .options(opts).mode(SaveMode.Append).save(path)
    makeDf(Seq(("t1", "unused", 3600L, "a-updated"))).write.format("hudi")
      .options(opts + (HoodieDataFrameWriter.OPERATION_KEY -> "upsert")).mode(SaveMode.Append).save(path)

    val rows = spark.read.format("hudi").load(path)
      .select("key", "_hoodie_partition_path", "value").collect()
      .map(r => (r.getString(0), r.getString(1), r.getString(2))).toSet
    Assertions.assertEquals(Set(
      ("t1", "19700101", "a-updated"),
      ("t2", "19700102", "b")), rows)
  }

  @Test
  def testUpsertWithoutPrecombineUsesCommitTimeOrdering(): Unit = {
    val path = tempDir.resolve("test_table_no_precombine").toString
    val opts = writeOptions("insert") - "hoodie.datasource.write.precombine.field"
    makeDf(Seq(("n1", "p1", 1L, "before"))).write.format("hudi")
      .options(opts).mode(SaveMode.Append).save(path)
    makeDf(Seq(("n1", "p1", 1L, "after"), ("n2", "p1", 1L, "dup-a"), ("n2", "p1", 2L, "dup-b")))
      .write.format("hudi")
      .options(opts + (HoodieDataFrameWriter.OPERATION_KEY -> "upsert")).mode(SaveMode.Append).save(path)

    val result = readAsMap(path)
    Assertions.assertEquals(2, result.size)
    Assertions.assertEquals("after", result("n1")._2)
    Assertions.assertTrue(Set("dup-a", "dup-b").contains(result("n2")._2))
    Assertions.assertEquals(2, completedCommits(path))
  }

  @Test
  def testSecondWriteBackfillsOptionsFromTableConfig(): Unit = {
    val path = tempDir.resolve("test_table_backfill").toString
    writeHudi(Seq(("b1", "p1", 1L, "one"), ("b2", "p2", 1L, "two")), "insert", path)

    // Only the path, the flag, and the operation: everything else comes from the table config.
    makeDf(Seq(("b1", "p1", 9L, "one-updated"))).write.format("hudi")
      .option(HoodieDataFrameWriter.DATAFRAME_WRITE_PATH_ENABLE, "true")
      .option(HoodieDataFrameWriter.OPERATION_KEY, "upsert")
      .option("hoodie.embed.timeline.server", "false")
      .mode(SaveMode.Append)
      .save(path)

    val result = readAsMap(path)
    Assertions.assertEquals(2, result.size)
    Assertions.assertEquals((9L, "one-updated", "b1"), result("b1"))
    Assertions.assertEquals(2, completedCommits(path))
  }

  @Test
  def testGlobalSimpleIndexMovesRecordAcrossPartitions(): Unit = {
    val path = tempDir.resolve("test_table_global_move").toString
    writeHudi(Seq(("g1", "pA", 1L, "orig"), ("g2", "pA", 1L, "keep")), "insert", path,
      Map("hoodie.index.type" -> "GLOBAL_SIMPLE"))
    // Default update-partition-path for the global simple index is true: the key moves to pB.
    writeHudi(Seq(("g1", "pB", 5L, "moved")), "upsert", path, Map("hoodie.index.type" -> "GLOBAL_SIMPLE"))

    val rows = spark.read.format("hudi").load(path)
      .select("key", "_hoodie_partition_path", "value").collect()
      .map(r => (r.getString(0), r.getString(1), r.getString(2))).toSet
    Assertions.assertEquals(Set(("g1", "pB", "moved"), ("g2", "pA", "keep")), rows)
  }

  @Test
  def testGlobalSimpleIndexUpdatesOldPartitionWhenMigrationDisabled(): Unit = {
    val path = tempDir.resolve("test_table_global_stay").toString
    val extra = Map(
      "hoodie.index.type" -> "GLOBAL_SIMPLE",
      "hoodie.simple.index.update.partition.path" -> "false")
    writeHudi(Seq(("s1", "pA", 1L, "orig")), "insert", path, extra)
    writeHudi(Seq(("s1", "pB", 5L, "updated")), "upsert", path, extra)

    val rows = spark.read.format("hudi").load(path)
      .select("key", "_hoodie_partition_path", "value").collect()
      .map(r => (r.getString(0), r.getString(1), r.getString(2))).toSet
    Assertions.assertEquals(Set(("s1", "pA", "updated")), rows)
  }

  @Test
  def testDeleteMarkerColumnDeletesRows(): Unit = {
    val path = tempDir.resolve("test_table_delete_marker").toString
    val deleteSchema = StructType(schema.fields :+ StructField("_hoodie_is_deleted", org.apache.spark.sql.types.BooleanType, nullable = false))
    def df(rows: Seq[(String, String, Long, String, Boolean)]) = spark.createDataFrame(
      rows.map(r => Row(r._1, r._2, r._3, r._4, r._5)).asJava, deleteSchema)

    df(Seq(("d1", "p1", 1L, "one", false), ("d2", "p1", 1L, "two", false)))
      .write.format("hudi").options(writeOptions("insert")).mode(SaveMode.Append).save(path)
    // d1 flagged for delete, d9 is a no-op delete of a missing key, d3 is a live insert.
    df(Seq(("d1", "p1", 2L, "gone", true), ("d9", "p1", 2L, "ghost", true), ("d3", "p1", 2L, "three", false)))
      .write.format("hudi").options(writeOptions("upsert")).mode(SaveMode.Append).save(path)

    val keys = spark.read.format("hudi").load(path).select("key").collect().map(_.getString(0)).toSet
    Assertions.assertEquals(Set("d2", "d3"), keys)
    Assertions.assertEquals(2, completedCommits(path))
  }

  @Test
  def testRecordIndexConfigFallsBackToGlobalTaggingWhenNotReady(): Unit = {
    val path = tempDir.resolve("test_table_rli_not_ready").toString
    writeHudi(Seq(("f1", "pA", 1L, "orig")), "insert", path)
    // The record index is configured but its metadata partition was never built; tagging keeps
    // global semantics (default update-partition-path for the record index is false).
    writeHudi(Seq(("f1", "pB", 5L, "updated")), "upsert", path, Map("hoodie.index.type" -> "RECORD_INDEX"))

    val rows = spark.read.format("hudi").load(path)
      .select("key", "_hoodie_partition_path", "value").collect()
      .map(r => (r.getString(0), r.getString(1), r.getString(2))).toSet
    Assertions.assertEquals(Set(("f1", "pA", "updated")), rows)
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
