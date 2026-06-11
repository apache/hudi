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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConverters._

/**
 * Incremental query correctness with the file group reader across source table versions and
 * read versions, COW and MOR (file slices with and without log files), and query shapes that
 * prune `_hoodie_commit_time` out of the scan schema (count(), isEmpty(), narrow projections)
 * where the incremental span filters must stay effective. Runs on a session without
 * HoodieSparkSessionExtension, so the filters are not injected into the logical plan and the
 * file format alone must keep the filter columns readable.
 */
class TestIncrementalReadWithFileGroupReader extends SparkClientFunctionalTestHarness {

  val columns: Seq[String] = Seq("ts", "key", "rider", "fare", "pt")

  // commits c1..c3 insert disjoint key pairs (small file handling keeps one file group with
  // base files only); commits c4..c6 update those pairs (log files on MOR)
  val batches: Seq[(Seq[(Int, String, String, Double, String)], String)] = Seq(
    (Seq((1, "k1", "rider-c1", 10.0, "pt1"), (1, "k2", "rider-c1", 10.0, "pt1")),
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL),
    (Seq((2, "k3", "rider-c2", 20.0, "pt1"), (2, "k4", "rider-c2", 20.0, "pt1")),
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL),
    (Seq((3, "k5", "rider-c3", 30.0, "pt1"), (3, "k6", "rider-c3", 30.0, "pt1")),
      DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL),
    (Seq((4, "k1", "rider-c4", 40.0, "pt1"), (4, "k2", "rider-c4", 40.0, "pt1")),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL),
    (Seq((5, "k3", "rider-c5", 50.0, "pt1"), (5, "k4", "rider-c5", 50.0, "pt1")),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL),
    (Seq((6, "k5", "rider-c6", 60.0, "pt1"), (6, "k6", "rider-c6", 60.0, "pt1")),
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL))

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,6,6",
    "COPY_ON_WRITE,8,6",
    "COPY_ON_WRITE,8,8",
    "MERGE_ON_READ,6,6",
    "MERGE_ON_READ,8,6",
    "MERGE_ON_READ,8,8"
  ))
  def testIncrementalReadRanges(tableType: String, sourceVersion: Int, readVersion: Int): Unit = {
    batches.zipWithIndex.foreach { case ((data, operation), i) =>
      val mode = if (i == 0) SaveMode.Overwrite else SaveMode.Append
      write(data, tableType, sourceVersion, operation, mode)
      if (i == 2) {
        // small file handling must have kept a single file group with base files only
        val (baseFiles, logFiles) = listDataFiles()
        Assertions.assertEquals(3, baseFiles.size, "Expected one base file per insert commit")
        Assertions.assertEquals(1, baseFiles.map(fileId).distinct.size, "Expected a single file group")
        Assertions.assertTrue(logFiles.isEmpty, "Expected no log files after insert-only commits")
      }
    }

    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf().newInstance()).setBasePath(basePath()).build()
    Assertions.assertEquals(sourceVersion, metaClient.getTableConfig.getTableVersion.versionCode())
    val (baseFiles, logFiles) = listDataFiles()
    Assertions.assertEquals(1, baseFiles.map(fileId).distinct.size, "Expected a single file group")
    if (tableType == "MERGE_ON_READ") {
      Assertions.assertEquals(3, baseFiles.size, "Update commits must not rewrite MOR base files")
      Assertions.assertEquals(3, logFiles.size, "Expected one log file per update commit")
    } else {
      Assertions.assertEquals(6, baseFiles.size, "Expected one base file per commit")
      Assertions.assertTrue(logFiles.isEmpty, "Expected no log files on COW")
    }

    // c1..c6 ordered by requested time
    val instants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
      .getInstants.asScala.toList
    Assertions.assertEquals(6, instants.size)

    // (000, c2]: base files only
    assertIncrementalRange(readVersion, instants, 0, 2,
      Set(("k1", 1), ("k2", 1), ("k3", 2), ("k4", 2)))
    // (c2, c4]: base file commit c3 plus, on MOR, the log file of c4; rows carried over into
    // the c3 base file from c1/c2 must be filtered out
    assertIncrementalRange(readVersion, instants, 2, 4,
      Set(("k5", 3), ("k6", 3), ("k1", 4), ("k2", 4)))
    // (c3, c5]: on MOR only the log files of c4/c5 are in range
    assertIncrementalRange(readVersion, instants, 3, 5,
      Set(("k1", 4), ("k2", 4), ("k3", 5), ("k4", 5)))
    // (c6, c6]: empty range
    assertIncrementalRange(readVersion, instants, 6, 6, Set.empty)
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
    Assertions.assertEquals(expected, rows)
    // projection without _hoodie_commit_time
    val keys = readIncremental(readVersion, start, end).select("key").collect().map(_.getString(0)).toSet
    Assertions.assertEquals(expected.map(_._1), keys)
    // these query shapes prune `_hoodie_commit_time` out of the scan schema
    Assertions.assertEquals(expected.size.toLong, readIncremental(readVersion, start, end).count())
    Assertions.assertEquals(expected.isEmpty, readIncremental(readVersion, start, end).isEmpty)
  }

  private def write(data: Seq[(Int, String, String, Double, String)], tableType: String,
                    sourceVersion: Int, operation: String, mode: SaveMode): Unit = {
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
      .mode(mode)
      .save(basePath)
  }

  private def readIncremental(readVersion: Int, start: String, end: String): DataFrame = {
    val reader = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key(), start)
      .option(DataSourceReadOptions.END_COMMIT.key(), end)
    val readerWithVersion = if (readVersion == 6) {
      // same access pattern as the S3/GCS event incremental sources
      reader.option(DataSourceReadOptions.INCREMENTAL_READ_TABLE_VERSION.key(), "6")
    } else {
      reader
    }
    readerWithVersion.load(basePath)
  }

  private def listDataFiles(): (Seq[String], Seq[String]) = {
    val names = fs.listStatus(new Path(basePath, "pt1")).map(_.getPath.getName).toSeq
    // log files are dot-prefixed: .{fileId}_{baseCommit}.log.{version}_{writeToken}
    (names.filter(n => !n.startsWith(".") && n.endsWith(".parquet")), names.filter(_.contains(".log.")))
  }

  private def fileId(baseFileName: String): String = baseFileName.substring(0, baseFileName.indexOf("_"))
}
