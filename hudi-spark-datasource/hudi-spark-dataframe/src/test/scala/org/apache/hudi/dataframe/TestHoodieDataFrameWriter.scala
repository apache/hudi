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

import org.apache.hudi.{DataSourceWriteOptions, HoodieDataFrameWritePath}
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
    // A stale hudi-spark-common on the classpath (e.g. reinstalled from another branch) cannot
    // reach this module, and every write below would silently exercise the RDD write path.
    Assertions.assertTrue(HoodieDataFrameWritePath.isAvailable,
      "hudi-spark-common on the test classpath does not resolve the DataFrame write path; "
        + "rebuild it from this branch")
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
    DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> "true",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    "hoodie.datasource.write.precombine.field" -> "ts",
    "hoodie.embed.timeline.server" -> "false",
    DataSourceWriteOptions.OPERATION.key -> operation) ++ extraOpts

  private def makeDf(rows: Seq[(String, String, Long, String)]) = {
    spark.createDataFrame(
      rows.map(r => Row(r._1, r._2, r._3, r._4)).asJava, schema)
  }

  private def writeHudi(rows: Seq[(String, String, Long, String)],
                        operation: String,
                        path: String,
                        extraOpts: Map[String, String] = Map.empty): Unit = {
    val options = writeOptions(operation, extraOpts)
    val expectedWrites = HoodieDataFrameWriter.writeCount.get() +
      (if (options(DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key).toBoolean) 1 else 0)
    makeDf(rows).write.format("hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(path)
    Assertions.assertEquals(expectedWrites, HoodieDataFrameWriter.writeCount.get(),
      "the write did not take the expected write path")
  }

  /**
   * Runs the same writes through the DataFrame write path and the RDD write path, asserts both
   * tables read back the same rows, and returns them.
   */
  private def writeOnBothPaths(name: String,
                               extraOpts: Map[String, String],
                               writes: Seq[(String, Seq[(String, String, Long, String)])]): Seq[(String, String, Long, String)] = {
    val snapshots = Seq("true", "false").map { enabled =>
      val path = tempDir.resolve(s"${name}_$enabled").toString
      val opts = extraOpts + (DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> enabled)
      writes.foreach { case (operation, rows) => writeHudi(rows, operation, path, opts) }
      spark.read.format("hudi").load(path).select("key", "partition", "ts", "value").collect()
        .map(r => (r.getString(0), r.getString(1), r.getLong(2), r.getString(3))).sorted.toSeq
    }
    Assertions.assertEquals(snapshots(1), snapshots.head)
    snapshots.head
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
  def testInsertPaddingKeepsDuplicateKeysLikeLegacyPath(): Unit = {
    val snapshots = Seq("true", "false").map { enabled =>
      val path = tempDir.resolve(s"test_table_insert_dups_$enabled").toString
      val opts = Map(DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> enabled)
      writeHudi(Seq(("d1", "p1", 1L, "x1"), ("d2", "p1", 1L, "x2")), "insert", path, opts)
      writeHudi(Seq(("d1", "p1", 2L, "y1"), ("d3", "p1", 2L, "y3")), "insert", path, opts)
      spark.read.format("hudi").load(path).select("key", "ts", "value").collect()
        .map(r => (r.getString(0), r.getLong(1), r.getString(2))).sorted.toSeq
    }
    // Inserts allow duplicate keys by default, so the second d1 lands next to the first one
    // even when it pads the same small file group.
    Assertions.assertEquals(
      Seq(("d1", 1L, "x1"), ("d1", 2L, "y1"), ("d2", 1L, "x2"), ("d3", 2L, "y3")), snapshots.head)
    Assertions.assertEquals(snapshots(1), snapshots.head)
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
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "key,partition",
      "hoodie.datasource.write.hive_style_partitioning" -> "true"))
    makeDf(Seq(("c1", "p1", 1L, "z1"), ("c2", "p2", 1L, "z2"))).write.format("hudi")
      .options(opts).mode(SaveMode.Append).save(path)
    makeDf(Seq(("c1", "p1", 5L, "z1-updated"))).write.format("hudi")
      .options(opts + (DataSourceWriteOptions.OPERATION.key -> "upsert")).mode(SaveMode.Append).save(path)

    val rows = spark.read.format("hudi").load(path)
      .select("_hoodie_record_key", "_hoodie_partition_path", "value").collect()
      .map(r => (r.getString(0), r.getString(1), r.getString(2))).toSet
    Assertions.assertEquals(Set(
      ("key:c1,partition:p1", "partition=p1", "z1-updated"),
      ("key:c2,partition:p2", "partition=p2", "z2")), rows)
  }

  @Test
  def testTimestampKeyGeneratorPartitioning(): Unit = {
    val snapshots = Seq("true", "false").map { enabled =>
      val path = tempDir.resolve(s"test_table_timestamp_$enabled").toString
      val opts = writeOptions("insert", Map(
        DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> enabled,
        "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.TimestampBasedKeyGenerator",
        DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "ts",
        "hoodie.keygen.timebased.timestamp.type" -> "UNIX_TIMESTAMP",
        "hoodie.keygen.timebased.output.dateformat" -> "yyyyMMdd",
        "hoodie.keygen.timebased.timezone" -> "UTC"))
      makeDf(Seq(("t1", "unused", 0L, "a"), ("t2", "unused", 86400L, "b"))).write.format("hudi")
        .options(opts).mode(SaveMode.Append).save(path)
      makeDf(Seq(("t1", "unused", 3600L, "a-updated"))).write.format("hudi")
        .options(opts + (DataSourceWriteOptions.OPERATION.key -> "upsert")).mode(SaveMode.Append).save(path)
      spark.read.format("hudi").load(path)
        .select("key", "_hoodie_partition_path", "value").collect()
        .map(r => (r.getString(0), r.getString(1), r.getString(2))).toSet
    }
    Assertions.assertEquals(Set(
      ("t1", "19700101", "a-updated"),
      ("t2", "19700102", "b")), snapshots.head)
    Assertions.assertEquals(snapshots(1), snapshots.head)
  }

  @Test
  def testUpsertWithoutPrecombineUsesCommitTimeOrdering(): Unit = {
    val snapshots = Seq("true", "false").map { enabled =>
      val path = tempDir.resolve(s"test_table_no_precombine_$enabled").toString
      val opts = writeOptions("insert") - "hoodie.datasource.write.precombine.field" +
        (DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> enabled)
      makeDf(Seq(("n1", "p1", 9L, "before"))).write.format("hudi")
        .options(opts).mode(SaveMode.Append).save(path)
      makeDf(Seq(("n1", "p1", 5L, "mid"), ("n2", "p1", 5L, "dup-a"), ("n2", "p1", 1L, "dup-b"), ("n1", "p1", 1L, "after")))
        .write.format("hudi")
        .options(opts + (DataSourceWriteOptions.OPERATION.key -> "upsert")).mode(SaveMode.Append).save(path)
      Assertions.assertEquals(2, completedCommits(path))
      readAsMap(path).map { case (key, (_, value, _)) => key -> value }
    }
    // With no ordering field, the row that comes last in the batch wins.
    Assertions.assertEquals(Map("n1" -> "after", "n2" -> "dup-b"), snapshots.head)
    Assertions.assertEquals(snapshots(1), snapshots.head)
  }

  @Test
  def testSecondWriteBackfillsOptionsFromTableConfig(): Unit = {
    val path = tempDir.resolve("test_table_backfill").toString
    writeHudi(Seq(("b1", "p1", 1L, "one"), ("b2", "p2", 1L, "two")), "insert", path)

    // Only the path, the flag, and the operation: everything else comes from the table config.
    makeDf(Seq(("b1", "p1", 9L, "one-updated"))).write.format("hudi")
      .option(DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key, "true")
      .option(DataSourceWriteOptions.OPERATION.key, "upsert")
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
  def testMergeOnReadInsertAndUpserts(): Unit = {
    val path = tempDir.resolve("test_table_mor").toString
    val mor = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.logfile.data.block.format" -> "parquet")

    writeHudi(Seq(("m1", "p1", 1L, "one"), ("m2", "p1", 1L, "two"), ("m3", "p2", 1L, "three")),
      "insert", path, mor)
    writeHudi(Seq(("m2", "p1", 5L, "two-updated"), ("m4", "p2", 2L, "four")), "upsert", path, mor)
    writeHudi(Seq(("m2", "p1", 9L, "two-final")), "upsert", path, mor)

    val result = readAsMap(path)
    Assertions.assertEquals(4, result.size)
    Assertions.assertEquals((1L, "one", "m1"), result("m1"))
    Assertions.assertEquals((9L, "two-final", "m2"), result("m2"))
    Assertions.assertEquals((1L, "three", "m3"), result("m3"))
    Assertions.assertEquals((2L, "four", "m4"), result("m4"))

    val metaClient = HoodieTableMetaClient.builder()
      .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
      .setBasePath(path)
      .build()
    Assertions.assertEquals("MERGE_ON_READ", metaClient.getTableConfig.getTableType.name())
    Assertions.assertEquals(3,
      metaClient.getActiveTimeline.getDeltaCommitTimeline.filterCompletedInstants().countInstants())

    val keys = spark.read.format("hudi").load(path).select("_hoodie_record_key").collect().map(_.getString(0))
    Assertions.assertEquals(keys.length, keys.distinct.length)
  }

  @Test
  def testMergeOnReadInsertsPadSmallBaseFiles(): Unit = {
    val path = tempDir.resolve("test_table_mor_padding").toString
    val mor = Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.logfile.data.block.format" -> "parquet")

    writeHudi(Seq(("mp1", "p1", 1L, "a"), ("mp2", "p1", 1L, "b")), "insert", path, mor)
    writeHudi(Seq(("mp3", "p1", 1L, "c"), ("mp4", "p1", 1L, "d")), "insert", path, mor)

    val snapshot = spark.read.format("hudi").load(path)
    Assertions.assertEquals(4, snapshot.count())
    val fileIds = snapshot.select("_hoodie_file_name").collect()
      .map(r => org.apache.hudi.common.fs.FSUtils.getFileId(r.getString(0))).distinct
    Assertions.assertEquals(1, fileIds.length)
  }

  @Test
  def testMergeOnReadWithAvroLogBlocks(): Unit = {
    val rows = writeOnBothPaths("test_table_mor_avro_logs",
      Map("hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
        "hoodie.logfile.data.block.format" -> "avro",
        "hoodie.compact.inline" -> "false"),
      Seq(
        "insert" -> Seq(("m1", "p1", 1L, "a1"), ("m2", "p1", 1L, "a2"), ("m3", "p2", 1L, "a3")),
        "upsert" -> Seq(("m1", "p1", 2L, "b1"), ("m4", "p2", 2L, "b4")),
        "upsert" -> Seq(("m1", "p1", 1L, "stale"), ("m3", "p2", 3L, "c3"))))
    Assertions.assertEquals(
      Seq(("m1", "p1", 2L, "b1"), ("m2", "p1", 1L, "a2"), ("m3", "p2", 3L, "c3"), ("m4", "p2", 2L, "b4")), rows)
  }

  @Test
  def testCustomPayloadMergesThroughThePayload(): Unit = {
    // The payload keeps the stored row when the incoming one is older, on the ordering field
    // it reads from the record, whichever write path builds the record.
    val rows = writeOnBothPaths("test_table_custom_payload",
      Map("hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.EventTimeAvroPayload",
        "hoodie.combine.before.upsert" -> "false",
        "hoodie.combine.before.insert" -> "false"),
      Seq(
        "insert" -> Seq(("c1", "p1", 5L, "a1"), ("c2", "p1", 5L, "a2")),
        "upsert" -> Seq(("c1", "p1", 9L, "newer"), ("c2", "p1", 1L, "older"), ("c3", "p1", 1L, "a3"))))
    Assertions.assertEquals(
      Seq(("c1", "p1", 9L, "newer"), ("c2", "p1", 5L, "a2"), ("c3", "p1", 1L, "a3")), rows)
  }

  @Test
  def testDeleteOperation(): Unit = {
    Seq("COPY_ON_WRITE", "MERGE_ON_READ").foreach { tableType =>
      val rows = writeOnBothPaths(s"test_table_delete_$tableType",
        Map("hoodie.datasource.write.table.type" -> tableType, "hoodie.compact.inline" -> "false"),
        Seq(
          "insert" -> Seq(("k1", "p1", 1L, "a1"), ("k2", "p1", 1L, "a2"), ("k3", "p2", 1L, "a3")),
          // k9 does not exist, and k3 is deleted twice in the same batch.
          "delete" -> Seq(("k1", "p1", 2L, "x"), ("k3", "p2", 2L, "x"), ("k3", "p2", 3L, "x"), ("k9", "p1", 2L, "x")),
          "upsert" -> Seq(("k1", "p1", 5L, "back"))))
      Assertions.assertEquals(Seq(("k1", "p1", 5L, "back"), ("k2", "p1", 1L, "a2")), rows)
    }
  }

  @Test
  def testInsertOverwriteReplacesTouchedPartitions(): Unit = {
    val rows = writeOnBothPaths("test_table_insert_overwrite", Map.empty,
      Seq(
        "insert" -> Seq(("o1", "p1", 1L, "a1"), ("o2", "p1", 1L, "a2"), ("o3", "p2", 1L, "a3")),
        "insert_overwrite" -> Seq(("o4", "p1", 2L, "b4")),
        "upsert" -> Seq(("o3", "p2", 3L, "c3"))))
    Assertions.assertEquals(Seq(("o3", "p2", 3L, "c3"), ("o4", "p1", 2L, "b4")), rows)
  }

  @Test
  def testInsertOverwriteTableReplacesEveryPartition(): Unit = {
    val rows = writeOnBothPaths("test_table_insert_overwrite_table", Map.empty,
      Seq(
        "insert" -> Seq(("w1", "p1", 1L, "a1"), ("w2", "p2", 1L, "a2")),
        "insert_overwrite_table" -> Seq(("w3", "p3", 2L, "b3"))))
    Assertions.assertEquals(Seq(("w3", "p3", 2L, "b3")), rows)
  }

  @Test
  def testInsertDropsKeysTheTableAlreadyHas(): Unit = {
    val rows = writeOnBothPaths("test_table_insert_drop_dups",
      Map(DataSourceWriteOptions.INSERT_DUP_POLICY.key -> DataSourceWriteOptions.DROP_INSERT_DUP_POLICY),
      Seq(
        "insert" -> Seq(("n1", "p1", 1L, "a1"), ("n2", "p1", 1L, "a2")),
        "insert" -> Seq(("n1", "p1", 2L, "dropped"), ("n3", "p1", 2L, "b3"))))
    Assertions.assertEquals(Seq(("n1", "p1", 1L, "a1"), ("n2", "p1", 1L, "a2"), ("n3", "p1", 2L, "b3")), rows)
  }

  @Test
  def testInsertFailsOnKeysTheTableAlreadyHas(): Unit = {
    Seq("true", "false").foreach { enabled =>
      val path = tempDir.resolve(s"test_table_insert_fail_dups_$enabled").toString
      val opts = Map(
        DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> enabled,
        DataSourceWriteOptions.INSERT_DUP_POLICY.key -> DataSourceWriteOptions.FAIL_INSERT_DUP_POLICY)
      writeHudi(Seq(("f1", "p1", 1L, "a1")), "insert", path, opts)
      val failure = Assertions.assertThrows(classOf[Exception], () =>
        makeDf(Seq(("f1", "p1", 2L, "dup"), ("f2", "p1", 2L, "b2"))).write.format("hudi")
          .options(writeOptions("insert", opts)).mode(SaveMode.Append).save(path))
      var cause: Throwable = failure
      while (cause != null && !cause.isInstanceOf[org.apache.hudi.exception.HoodieDuplicateKeyException]) {
        cause = cause.getCause
      }
      Assertions.assertNotNull(cause, s"expected a duplicate key failure, got $failure")
      Assertions.assertEquals(1L, spark.read.format("hudi").load(path).count())
    }
  }

  @Test
  def testUpsertCombinesOnStringOrderingField(): Unit = {
    // Two rows of a new key and two of an existing key in one batch: the row with the greater
    // ordering value wins, and it pads the small file group of the first commit.
    val rows = writeOnBothPaths("test_table_string_ordering",
      Map("hoodie.datasource.write.precombine.field" -> "value"),
      Seq(
        "insert" -> Seq(("s1", "p1", 1L, "a1")),
        "upsert" -> Seq(("s1", "p1", 1L, "a1_1"), ("s2", "p1", 1L, "a2"), ("s2", "p1", 1L, "a2_2"), ("s1", "p1", 1L, "a0"))))
    Assertions.assertEquals(Seq(("s1", "p1", 1L, "a1_1"), ("s2", "p1", 1L, "a2_2")), rows)
  }

  @Test
  def testPayloadClassCombinesRecordsOfAKey(): Unit = {
    // The payload class merges on its own terms, so the rows of a key are combined with the
    // table's record merger, for an existing key and for a new one.
    Seq("COPY_ON_WRITE", "MERGE_ON_READ").foreach { tableType =>
      val rows = writeOnBothPaths(s"test_table_payload_combine_$tableType",
        Map("hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.PartialUpdateAvroPayload",
          "hoodie.datasource.write.table.type" -> tableType,
          "hoodie.compact.inline" -> "false"),
        Seq(
          "insert" -> Seq(("q1", "p1", 5L, "a1")),
          "upsert" -> Seq(("q1", "p1", 9L, "newer"), ("q1", "p1", 7L, "older"), ("q2", "p1", 3L, "x"), ("q2", "p1", 4L, "y"))))
      Assertions.assertEquals(Seq(("q1", "p1", 9L, "newer"), ("q2", "p1", 4L, "y")), rows)
    }
  }

  @Test
  def testInsertsAreSizedIntoFilesLikeTheLegacyPath(): Unit = {
    // 100 KB files at the default 1 KB record size estimate: 100 records per new file.
    val fileCounts = Seq("true", "false").map { enabled =>
      val path = tempDir.resolve(s"test_table_insert_sizing_$enabled").toString
      val opts = Map(
        DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> enabled,
        "hoodie.parquet.max.file.size" -> (100 * 1024).toString,
        "hoodie.parquet.small.file.limit" -> (50 * 1024).toString)
      def fileIds(): Set[String] = spark.read.format("hudi").load(path).select("_hoodie_file_name").distinct().collect()
        .map(r => org.apache.hudi.common.fs.FSUtils.getFileId(r.getString(0))).toSet

      writeHudi((1 to 1000).map(i => (s"z$i", "p1", 1L, s"v$i")), "insert", path, opts)
      val afterFirst = fileIds()
      writeHudi((1001 to 1010).map(i => (s"z$i", "p2", 1L, s"v$i")), "insert", path, opts)
      val afterSecond = fileIds()
      writeHudi((1011 to 1012).map(i => (s"z$i", "p2", 1L, s"v$i")), "insert", path, opts)
      val afterThird = fileIds()
      Assertions.assertEquals(1012L, spark.read.format("hudi").load(path).count())
      Seq(afterFirst.size, afterSecond.size, afterThird.size)
    }
    Assertions.assertEquals(10, fileCounts.head.head)
    Assertions.assertEquals(fileCounts(1), fileCounts.head)
  }

  @Test
  def testNullOrderingValueFailsTheWrite(): Unit = {
    val nullableSchema = StructType(schema.map(_.copy(nullable = true)))
    Seq("true", "false").foreach { enabled =>
      val path = tempDir.resolve(s"test_table_null_ordering_$enabled").toString
      val df = spark.createDataFrame(Seq(Row("u1", "p1", null, "a1")).asJava, nullableSchema)
      val failure = Assertions.assertThrows(classOf[Exception], () =>
        df.write.format("hudi")
          .options(writeOptions("upsert", Map(DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> enabled)))
          .mode(SaveMode.Append).save(path))
      var cause: Throwable = failure
      while (cause.getCause != null) {
        cause = cause.getCause
      }
      Assertions.assertTrue(cause.isInstanceOf[IllegalArgumentException], s"unexpected failure: $cause")
      Assertions.assertTrue(cause.getMessage.contains("Ordering field 'ts' has null value for record key 'u1'"),
        cause.getMessage)
    }
  }

  @Test
  def testMissingRecordKeyFieldFailsWithTheSameError(): Unit = {
    Seq("missing", "key,missing").foreach { recordKeyFields =>
      val messages = Seq("true", "false").map { enabled =>
        val path = tempDir.resolve(s"test_table_missing_key_${recordKeyFields.hashCode}_$enabled").toString
        val failure = Assertions.assertThrows(classOf[Exception], () =>
          makeDf(Seq(("e1", "p1", 1L, "a1"))).write.format("hudi")
            .options(writeOptions("insert", Map(
              DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> enabled,
              DataSourceWriteOptions.RECORDKEY_FIELD.key -> recordKeyFields)))
            .mode(SaveMode.Append).save(path))
        var cause: Throwable = failure
        while (cause.getCause != null) {
          cause = cause.getCause
        }
        cause.getMessage
      }
      Assertions.assertTrue(messages.head.contains("missing"), messages.head)
      Assertions.assertEquals(messages(1), messages.head)
    }
  }

  @Test
  def testInputWithoutColumnsCommitsNothing(): Unit = {
    Seq("true", "false").foreach { enabled =>
      val path = tempDir.resolve(s"test_table_no_columns_$enabled").toString
      val opts = Map(DataSourceWriteOptions.DATAFRAME_WRITE_PATH_ENABLE.key -> enabled)
      writeHudi(Seq(("g1", "p1", 1L, "a1")), "insert", path, opts)
      spark.emptyDataFrame.write.format("hudi")
        .options(writeOptions("upsert", opts)).mode(SaveMode.Append).save(path)
      Assertions.assertEquals(2, completedCommits(path))
      Assertions.assertEquals(1L, spark.read.format("hudi").load(path).count())
    }
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
