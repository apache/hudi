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

package org.apache.hudi.functional

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, MergeOnReadIncrementalRelationV2}
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig, TimestampKeyGeneratorConfig}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.read.IncrementalQueryAnalyzer.START_COMMIT_EARLIEST
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.{CustomKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, ValueSource}

import java.util.UUID

import scala.collection.JavaConverters._

/**
 * Incremental query correctness with the file group reader across COW/MOR, source table
 * versions, read versions, and query shapes that prune `_hoodie_commit_time` from the scan
 * schema (count(), isEmpty(), narrow projections). Runs without HoodieSparkSessionExtension,
 * so the file format alone must keep the span-filter columns readable.
 */
class TestIncrementalReadWithFileGroupReader extends SparkClientFunctionalTestHarness {

  val columns: Seq[String] = Seq("ts", "key", "rider", "fare", "pt")

  // c1..c3 insert disjoint key pairs (one file group, base files only via small file handling);
  // c4..c6 are update commits (log files on MOR), each updating k1 with a different value so a
  // range must surface only the targeted update of k1
  val batches: Seq[(Seq[(Int, String, String, Double, String)], String)] = Seq(
    (Seq((1, "k1", "rider-c1", 10.0, "pt1"), (1, "k2", "rider-c1", 10.0, "pt1")),
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL),
    (Seq((2, "k3", "rider-c2", 20.0, "pt1"), (2, "k4", "rider-c2", 20.0, "pt1")),
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL),
    (Seq((3, "k5", "rider-c3", 30.0, "pt1"), (3, "k6", "rider-c3", 30.0, "pt1")),
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL),
    (Seq((4, "k1", "rider-c4", 40.0, "pt1"), (4, "k2", "rider-c4", 40.0, "pt1")),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL),
    (Seq((5, "k1", "rider-c5", 50.0, "pt1"), (5, "k3", "rider-c5", 50.0, "pt1"), (5, "k4", "rider-c5", 50.0, "pt1")),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL),
    (Seq((6, "k1", "rider-c6", 60.0, "pt1"), (6, "k5", "rider-c6", 60.0, "pt1"), (6, "k6", "rider-c6", 60.0, "pt1")),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL))

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,6,6,PARQUET",
    "COPY_ON_WRITE,8,6,PARQUET",
    "COPY_ON_WRITE,8,8,PARQUET",
    "COPY_ON_WRITE,6,6,ORC",
    "COPY_ON_WRITE,8,8,ORC",
    "MERGE_ON_READ,6,6,PARQUET",
    "MERGE_ON_READ,6,8,PARQUET",
    "MERGE_ON_READ,8,6,PARQUET",
    "MERGE_ON_READ,8,8,PARQUET"
  ))
  def testIncrementalReadRanges(tableType: String, sourceVersion: Int, readVersion: Int, baseFileFormat: String): Unit = {
    batches.zipWithIndex.foreach { case ((data, operation), i) =>
      val mode = if (i == 0) SaveMode.Overwrite else SaveMode.Append
      write(data, tableType, sourceVersion, operation, mode, Map(HoodieTableConfig.BASE_FILE_FORMAT.key -> baseFileFormat))
      if (i == 2) {
        // small file handling must have kept a single file group with base files only
        val (baseFiles, logFiles) = listDataFiles()
        assertEquals(3, baseFiles.size, "Expected one base file per insert commit")
        assertEquals(1, baseFiles.map(FSUtils.getFileId).distinct.size, "Expected a single file group")
        assertTrue(logFiles.isEmpty, "Expected no log files after insert-only commits")
      }
    }

    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf().newInstance()).setBasePath(basePath()).build()
    assertEquals(sourceVersion, metaClient.getTableConfig.getTableVersion.versionCode())
    val (baseFiles, logFiles) = listDataFiles()
    assertEquals(1, baseFiles.map(FSUtils.getFileId).distinct.size, "Expected a single file group")
    if (tableType == "MERGE_ON_READ") {
      assertEquals(3, baseFiles.size, "Update commits must not rewrite MOR base files")
      assertEquals(3, logFiles.size, "Expected one log file per update commit")
    } else {
      assertEquals(6, baseFiles.size, "Expected one base file per commit")
      assertTrue(logFiles.isEmpty, "Expected no log files on COW")
    }
    // records merged into the latest base file keep their original commit times
    val latestBaseFile = baseFiles.maxBy(name => FSUtils.getCommitTime(name))
    val commitTimesInBaseFile = spark.read.format(baseFileFormat.toLowerCase)
      .load(new Path(new Path(basePath, "pt1"), latestBaseFile).toString)
      .select("_hoodie_commit_time").distinct().count()
    assertTrue(commitTimesInBaseFile > 1,
      s"Expected multiple commit times in the latest base file, got $commitTimesInBaseFile")

    // c1..c6 ordered by requested time
    val instants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
      .getInstants.asScala.toList
    assertEquals(6, instants.size)

    // (000, c2]: base files only
    assertIncrementalRange(readVersion, instants, 0, 2,
      Set(("k1", 1), ("k2", 1), ("k3", 2), ("k4", 2)))
    // (c1, c2]: single base file in range
    assertIncrementalRange(readVersion, instants, 1, 2,
      Set(("k3", 2), ("k4", 2)))
    // (c2, c4]: base file of c3 plus c4's log file on MOR; carried-over c1/c2 rows filtered out
    assertIncrementalRange(readVersion, instants, 2, 4,
      Set(("k5", 3), ("k6", 3), ("k1", 4), ("k2", 4)))
    // (c3, c5]: log files of c4/c5 only on MOR; k1 updated in both c4 and c5 must surface once
    // with the latest in-range value
    assertIncrementalRange(readVersion, instants, 3, 5,
      Set(("k1", 5), ("k2", 4), ("k3", 5), ("k4", 5)))
    // (c6, c6]: empty range
    assertIncrementalRange(readVersion, instants, 6, 6, Set.empty)
  }

  /**
   * COW incremental reads are splittable: a range read whose base file is split into several
   * input partitions must return the same rows as the unsplit read, with the commit-time range
   * enforced in every split.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("PARQUET", "ORC"))
  def testCowIncrementalReadWithMultipleSplitsPerFile(baseFileFormat: String): Unit = {
    val options = Map(
      HoodieTableConfig.BASE_FILE_FORMAT.key -> baseFileFormat,
      HoodieStorageConfig.PARQUET_BLOCK_SIZE.key -> "65536",
      HoodieStorageConfig.PARQUET_PAGE_SIZE.key -> "8192",
      HoodieStorageConfig.ORC_STRIPE_SIZE.key -> "65536",
      HoodieStorageConfig.ORC_BLOCK_SIZE.key -> "65536")
    def rows(commit: Int, keys: Range): Seq[(Int, String, String, Double, String)] = keys.map { i =>
      (commit, f"k$i%05d", UUID.nameUUIDFromBytes(s"$i-$commit".getBytes).toString * 3, commit.toDouble, "pt1")
    }
    val c1Keys = 0 until 2000
    val c2Keys = 2000 until 4000
    val c3Keys = 4000 until 6000
    // c1..c3 insert disjoint keys into one file group; c4 updates the keys of c1, so every base file
    // from c2 on carries rows of earlier commits that the range must filter out
    write(rows(1, c1Keys), "COPY_ON_WRITE", 8, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite, options)
    write(rows(2, c2Keys), "COPY_ON_WRITE", 8, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Append, options)
    write(rows(3, c3Keys), "COPY_ON_WRITE", 8, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Append, options)
    write(rows(4, c1Keys), "COPY_ON_WRITE", 8, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, SaveMode.Append, options)
    val (baseFiles, _) = listDataFiles()
    assertEquals(1, baseFiles.map(FSUtils.getFileId).distinct.size, "Expected a single file group")

    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf().newInstance()).setBasePath(basePath()).build()
    val instants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
      .getInstants.asScala.toList
    assertEquals(4, instants.size)

    def keysAndTs(df: DataFrame): Set[(String, Int)] =
      df.select("key", "ts").collect().map(r => (r.getString(0), r.getInt(1))).toSet

    // (c1, c3]: c2 and c3 rows, without the c1 rows carried over in the same base file
    val c1c3 = (instants(0).getCompletionTime, instants(2).getCompletionTime)
    val expectedC1C3 = c2Keys.map(i => (f"k$i%05d", 2)).toSet ++ c3Keys.map(i => (f"k$i%05d", 3)).toSet
    // (c3, c4]: only the c4 updates of the c1 keys
    val c3c4 = (instants(2).getCompletionTime, instants(3).getCompletionTime)
    val expectedC3C4 = c1Keys.map(i => (f"k$i%05d", 4)).toSet
    assertEquals(expectedC1C3, keysAndTs(readIncremental(8, c1c3._1, c1c3._2)))
    assertEquals(expectedC3C4, keysAndTs(readIncremental(8, c3c4._1, c3c4._2)))

    spark.conf.set("spark.sql.files.maxPartitionBytes", "65536")
    try {
      Seq((c1c3, expectedC1C3), (c3c4, expectedC3C4)).foreach { case ((start, end), expected) =>
        val df = readIncremental(8, start, end)
        assertTrue(df.rdd.getNumPartitions > df.inputFiles.length,
          s"Expected more input partitions than files, got ${df.rdd.getNumPartitions} for ${df.inputFiles.length} files")
        assertEquals(expected, keysAndTs(df))
        assertEquals(expected.size.toLong, readIncremental(8, start, end).count())
      }
    } finally {
      spark.conf.unset("spark.sql.files.maxPartitionBytes")
    }
  }

  /**
   * COW incremental reads where a partition column is read from the base file rather than the
   * partition path (timestamp-based partitioning), so the commit-time range is evaluated against a
   * read schema that carries the mandatory partition fields. TIMESTAMP reads every partition column
   * from the file; CUSTOM reads one from the file and appends the other from the path.
   */
  @ParameterizedTest
  @ValueSource(strings = Array("TIMESTAMP", "CUSTOM"))
  def testCowIncrementalReadWithPartitionValuesReadFromFile(keyGenType: String): Unit = {
    val (keyGenClass, partitionFields) = keyGenType match {
      case "TIMESTAMP" => (classOf[TimestampBasedKeyGenerator].getName, "event_ts")
      case "CUSTOM" => (classOf[CustomKeyGenerator].getName, "pt:SIMPLE,event_ts:TIMESTAMP")
    }
    // all records fall on 2023-11-14 UTC so that they share one partition and file group
    def write(data: Seq[(Int, String, String, Long, String)], operation: String, mode: SaveMode): Unit = {
      spark.createDataFrame(data).toDF("ts", "key", "rider", "event_ts", "pt").write.format("hudi")
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "key")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, partitionFields)
        .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, keyGenClass)
        .option(TimestampKeyGeneratorConfig.TIMESTAMP_TYPE_FIELD.key, "EPOCHMILLISECONDS")
        .option(TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_DATE_FORMAT.key, "yyyyMMdd")
        .option(TimestampKeyGeneratorConfig.TIMESTAMP_OUTPUT_TIMEZONE_FORMAT.key, "UTC")
        .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
        .option(DataSourceWriteOptions.TABLE_TYPE.key, "COPY_ON_WRITE")
        .option(DataSourceWriteOptions.TABLE_NAME.key, "test_incr_read_fgr_keygen")
        .option(DataSourceWriteOptions.OPERATION.key, operation)
        .option("hoodie.insert.shuffle.parallelism", "2")
        .option("hoodie.upsert.shuffle.parallelism", "2")
        .mode(mode)
        .save(basePath)
    }
    def eventTs(ts: Int): Long = 1699920000000L + ts * 1000L
    write(Seq((1, "k1", "rider-c1", eventTs(1), "pt1"), (1, "k2", "rider-c1", eventTs(1), "pt1")),
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Overwrite)
    write(Seq((2, "k3", "rider-c2", eventTs(2), "pt1"), (2, "k4", "rider-c2", eventTs(2), "pt1")),
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, SaveMode.Append)
    // c3 rewrites the base file with k1 updated, carrying the c1 and c2 rows over
    write(Seq((3, "k1", "rider-c3", eventTs(1), "pt1")),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, SaveMode.Append)

    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf().newInstance()).setBasePath(basePath()).build()
    val instants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
      .getInstants.asScala.toList
    assertEquals(3, instants.size)

    // event_ts is compared as text: the timestamp key generator types it as a string partition column
    def assertRange(start: String, end: String, expected: Set[(String, Int, Long, String)]): Unit = {
      val rows = readIncremental(8, start, end).select("key", "ts", "event_ts", "pt").collect()
        .map(r => (r.getString(0), r.getInt(1), String.valueOf(r.get(2)), r.getString(3))).toSet
      assertEquals(expected.map(e => (e._1, e._2, e._3.toString, e._4)), rows)
      assertEquals(expected.size.toLong, readIncremental(8, start, end).count())
    }
    // (000, c2]: the rows as of c2, including the c1 version of k1 that c3 later updates
    assertRange("000", instants(1).getCompletionTime,
      Set(("k1", 1, eventTs(1), "pt1"), ("k2", 1, eventTs(1), "pt1"), ("k3", 2, eventTs(2), "pt1"),
        ("k4", 2, eventTs(2), "pt1")))
    // (c1, c2]: the c2 rows only
    assertRange(instants(0).getCompletionTime, instants(1).getCompletionTime,
      Set(("k3", 2, eventTs(2), "pt1"), ("k4", 2, eventTs(2), "pt1")))
    // (c2, c3]: the c3 update of k1
    assertRange(instants(1).getCompletionTime, instants(2).getCompletionTime,
      Set(("k1", 3, eventTs(1), "pt1")))
  }

  @ParameterizedTest(name = "V2 MOR full-table scan applies instant range, table version = {0}, legacy RDD = {1}")
  @CsvSource(value = Array("6,false", "6,true", "8,false", "8,true"))
  def testV2MorFullTableScanAppliesInstantRange(sourceVersion: Int, useLegacyRdd: Boolean): Unit = {
    val archivalOptions = Map(
      HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key -> "2",
      HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key -> "5",
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key -> "1",
      HoodieCleanConfig.AUTO_CLEAN.key -> "false",
      HoodieMetadataConfig.ENABLE.key -> "false",
      HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false")

    batches.zipWithIndex.foreach { case ((data, operation), i) =>
      write(data, "MERGE_ON_READ", sourceVersion, operation,
        if (i == 0) SaveMode.Overwrite else SaveMode.Append, archivalOptions)
    }

    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf().newInstance()).setBasePath(basePath()).build()
    assertEquals(sourceVersion, metaClient.getTableConfig.getTableVersion.versionCode())
    val archivedInstants = metaClient.getArchivedTimeline.getCommitsTimeline.filterCompletedInstants
      .getInstants.asScala.toList
    val activeInstants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
      .getInstants.asScala.toList
    val instants = (archivedInstants ++ activeInstants)
      .map(instant => instant.requestedTime -> instant).toMap.values.toList.sortBy(_.requestedTime)

    assertTrue(archivedInstants.nonEmpty, "The query must include archived instants to force a full-table scan")
    assertEquals(6, instants.size)
    assertTrue(instants.last.requestedTime.compareTo(instants(4).requestedTime) > 0,
      "c6 must be outside the requested range")
    val (_, logFiles) = listDataFiles()
    assertEquals(3, logFiles.size, "The out-of-range c6 log must exist physically")

    def queryBoundary(instant: HoodieInstant): String = {
      if (sourceVersion == 6) instant.requestedTime else instant.getCompletionTime
    }

    val c3Boundary = queryBoundary(instants(2))
    val c5Boundary = queryBoundary(instants(4))
    val expectedThroughC5 = Set(
      ("k1", 5), ("k2", 4), ("k3", 5), ("k4", 5), ("k5", 3), ("k6", 3))

    // The earliest marker produces a closed range with a nullable lower boundary.
    assertFullTableScanRange(metaClient, useLegacyRdd, START_COMMIT_EARLIEST, c5Boundary, expectedThroughC5)
    // Starting with a concrete boundary produces an exact requested-time range. Verify both
    // the upper bound and a bounded lower range while the latest file slice still contains c6.
    assertFullTableScanRange(metaClient, useLegacyRdd, "000", c5Boundary, expectedThroughC5)
    assertFullTableScanRange(metaClient, useLegacyRdd, c3Boundary, c5Boundary,
      Set(("k1", 5), ("k2", 4), ("k3", 5), ("k4", 5)))
  }

  private def assertFullTableScanRange(metaClient: HoodieTableMetaClient,
                                       useLegacyRdd: Boolean,
                                       start: String,
                                       end: String,
                                       expected: Set[(String, Int)]): Unit = {
    val readOptions = Map(
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      DataSourceReadOptions.START_COMMIT.key -> start,
      DataSourceReadOptions.END_COMMIT.key -> end,
      DataSourceReadOptions.INCREMENTAL_READ_TABLE_VERSION.key -> "8",
      DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key -> "true")
    val result = if (useLegacyRdd) {
      spark.baseRelationToDataFrame(
        MergeOnReadIncrementalRelationV2(spark.sqlContext, readOptions + ("path" -> basePath), metaClient, None))
    } else {
      spark.read.format("hudi").options(readOptions).load(basePath)
    }

    val actual = result.select("key", "ts").collect()
      .map(row => (row.getString(0), row.getInt(1))).toSet
    assertEquals(expected, actual)
  }

  private def assertIncrementalRange(readVersion: Int,
                                     instants: List[HoodieInstant],
                                     startIdx: Int, endIdx: Int,
                                     expected: Set[(String, Int)]): Unit = {
    def boundary(idx: Int): String = {
      if (idx == 0) {
        "000"
      } else if (readVersion == 6) {
        instants(idx - 1).requestedTime
      } else {
        instants(idx - 1).getCompletionTime
      }
    }
    val start = boundary(startIdx)
    val end = boundary(endIdx)

    // select *
    val rows = readIncremental(readVersion, start, end).collect()
      .map(r => (r.getAs[String]("key"), r.getAs[Int]("ts"))).toSet
    assertEquals(expected, rows)
    // projection without _hoodie_commit_time
    val keys = readIncremental(readVersion, start, end).select("key").collect().map(_.getString(0)).toSet
    assertEquals(expected.map(_._1), keys)
    // these query shapes prune `_hoodie_commit_time` out of the scan schema
    assertEquals(expected.size.toLong, readIncremental(readVersion, start, end).count())
    assertEquals(expected.isEmpty, readIncremental(readVersion, start, end).isEmpty)
  }

  private def write(data: Seq[(Int, String, String, Double, String)], tableType: String,
                    sourceVersion: Int, operation: String, mode: SaveMode,
                    additionalOptions: Map[String, String] = Map.empty): Unit = {
    spark.createDataFrame(data).toDF(columns: _*).write.format("hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "key")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "pt")
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
      .option(DataSourceWriteOptions.TABLE_NAME.key, "test_incr_read_fgr")
      .option(HoodieWriteConfig.WRITE_TABLE_VERSION.key, sourceVersion.toString)
      .option(HoodieCompactionConfig.INLINE_COMPACT.key, "false")
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .options(additionalOptions)
      .mode(mode)
      .save(basePath)
  }

  private def readIncremental(readVersion: Int, start: String, end: String): DataFrame = {
    val reader = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key(), start)
      .option(DataSourceReadOptions.END_COMMIT.key(), end)
      // Explicitly exercise the selected reader version, including V2 reads of V6 tables.
      .option(DataSourceReadOptions.INCREMENTAL_READ_TABLE_VERSION.key(), readVersion.toString)
    reader.load(basePath)
  }

  private def listDataFiles(): (Seq[String], Seq[String]) = {
    val names = fs.listStatus(new Path(basePath, "pt1")).map(_.getPath.getName).toSeq
    (names.filter(n => FSUtils.isBaseFile(new StoragePath(n))), names.filter(n => FSUtils.isLogFile(n)))
  }
}
