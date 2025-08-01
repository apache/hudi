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

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.DataSourceReadOptions.{START_OFFSET, STREAMING_READ_TABLE_VERSION}
import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.model.HoodieTableType.{COPY_ON_WRITE, MERGE_ON_READ}
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieCompactionConfig}
import org.apache.hudi.config.HoodieWriteConfig.{DELETE_PARALLELISM_VALUE, INSERT_PARALLELISM_VALUE, TBL_NAME, UPSERT_PARALLELISM_VALUE, WRITE_TABLE_VERSION}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.util.JavaConversions

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.streaming.StreamTest
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

class TestStreamingSource extends StreamTest {

  import testImplicits._
  protected val commonOptions: Map[String, String] = Map(
    RECORDKEY_FIELD.key -> "id",
    PRECOMBINE_FIELD.key -> "ts",
    INSERT_PARALLELISM_VALUE.key -> "4",
    UPSERT_PARALLELISM_VALUE.key -> "4",
    DELETE_PARALLELISM_VALUE.key -> "4"
  )
  private val columns = Seq("id", "name", "price", "ts")

  org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)

  override protected def sparkConf = {
    super.sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
  }

  test("test cow stream source") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      HoodieTableMetaClient.newTableBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      val df = spark.readStream
        .format("org.apache.hudi")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "10", "000")), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("1", "a1", "12", "000"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "12", "000")), lastOnly = true, isSorted = false),

        addDataToQuery(tablePath,
          Seq(("2", "a2", "12", "000"),
            ("3", "a3", "12", "000"),
            ("4", "a4", "12", "000"))),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("2", "a2", "12", "000"),
            Row("3", "a3", "12", "000"),
            Row("4", "a4", "12", "000")),
          lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("5", "a5", "12", "000"))),
        addDataToQuery(tablePath, Seq(("6", "a6", "12", "000"))),
        addDataToQuery(tablePath, Seq(("5", "a5", "15", "000"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("6", "a6", "12", "000"),
            Row("5", "a5", "15", "000")),
          lastOnly = true, isSorted = false)
      )
    }
  }

  test("test mor stream source") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_mor_stream"
      HoodieTableMetaClient.newTableBuilder()
        .setTableType(MERGE_ON_READ)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      val df = spark.readStream
        .format("org.apache.hudi")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("1", "a1", "10", "000")), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath,
          Seq(("2", "a2", "12", "000"),
            ("3", "a3", "12", "000"),
            ("2", "a2", "10", "001"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("3", "a3", "12", "000"),
            Row("2", "a2", "10", "001")),
          lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("5", "a5", "12", "000"))),
        addDataToQuery(tablePath, Seq(("6", "a6", "12", "000"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(
          Seq(Row("5", "a5", "12", "000"),
            Row("6", "a6", "12", "000")),
          lastOnly = true, isSorted = false)
      )
    }
  }

  test("Test cow from latest offset") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      HoodieTableMetaClient.newTableBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      val df = spark.readStream
        .format("org.apache.hudi")
        .option(START_OFFSET.key(), "latest")
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        // Start from the latest, should contains no data
        CheckAnswerRows(Seq(), lastOnly = true, isSorted = false),
        StopStream,

        addDataToQuery(tablePath, Seq(("2", "a1", "12", "000"))),
        StartStream(),
        AssertOnQuery { q => q.processAllAvailable(); true },
        CheckAnswerRows(Seq(Row("2", "a1", "12", "000")), lastOnly = false, isSorted = false)
      )
    }
  }

  test("Test cow from specified offset") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream"
      val metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      addData(tablePath, Seq(("2", "a1", "11", "001")))
      addData(tablePath, Seq(("3", "a1", "12", "002")))

      val timestamp =
        metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()
          .firstInstant().get().getCompletionTime

      val df = spark.readStream
        .format("org.apache.hudi")
        .option(START_OFFSET.key(), timestamp)
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        // Start after the first commit
        CheckAnswerRows(Seq(Row("2", "a1", "11", "001"), Row("3", "a1", "12", "002")), lastOnly = true, isSorted = false)
      )
    }
  }

  test("Test mor streaming source with clustering") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_mor_stream_cluster"
      val metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(MERGE_ON_READ)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      addData(tablePath, Seq(("2", "a1", "11", "001")))
      addData(tablePath, Seq(("3", "a1", "12", "002")))
      addData(tablePath, Seq(("4", "a1", "13", "003")), enableInlineCluster = true)
      addData(tablePath, Seq(("5", "a1", "14", "004")))

      val timestamp =
        metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()
          .firstInstant().get().getCompletionTime

      val df = spark.readStream
        .format("org.apache.hudi")
        .option(START_OFFSET.key(), timestamp)
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        // Start after the first commit
        CheckAnswerRows(Seq(
          Row("2", "a1", "11", "001"),
          Row("3", "a1", "12", "002"),
          Row("4", "a1", "13", "003"),
          Row("5", "a1", "14", "004")), lastOnly = true, isSorted = false))
      assertTrue(metaClient.reloadActiveTimeline
        .filter(JavaConversions.getPredicate(
          e => e.isCompleted && HoodieTimeline.REPLACE_COMMIT_ACTION.equals(e.getAction)))
        .countInstants() > 0)
    }
  }

  test("test mor stream source with compaction") {
    Array("true", "false").foreach(skipCompact => {
      withTempDir { inputDir =>
        val tablePath = s"${inputDir.getCanonicalPath}/test_mor_stream_$skipCompact"
        val metaClient = HoodieTableMetaClient.newTableBuilder()
          .setTableType(MERGE_ON_READ)
          .setTableName(getTableName(tablePath))
          .setRecordKeyFields("id")
          .setPreCombineField("ts")
          .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

        addData(tablePath, Seq(("1", "a1", "10", "000")))
        val df = spark.readStream
          .format("org.apache.hudi")
          .option(DataSourceReadOptions.INCREMENTAL_READ_SKIP_COMPACT.key(), skipCompact)
          .load(tablePath)
          .select("id", "name", "price", "ts")

        addData(tablePath,
          Seq(("1", "a2", "12", "000"),
            ("2", "a3", "12", "000")))
        addData(tablePath, Seq(("2", "a5", "12", "000"), ("1", "a6", "12", "001")))
        // trigger compaction
        addData(tablePath, Seq(("3", "a6", "12", "000")), enableInlineCompaction = true)

        testStream(df)(
          AssertOnQuery {q => q.processAllAvailable(); true },
          CheckAnswerRows(Seq(Row("1", "a6", "12", "001"),
            Row("2", "a5", "12", "000"),
            Row("3", "a6", "12", "000")), lastOnly = true, isSorted = false),
          StopStream
        )
        assertTrue(metaClient.reloadActiveTimeline
          .filter(JavaConversions.getPredicate(
            e => e.isCompleted && HoodieTimeline.COMMIT_ACTION.equals(e.getAction)))
          .countInstants() > 0)
      }
    })
  }

  test("Test checkpoint translation") {
    withTempDir { inputDir =>
      val tablePath = s"${inputDir.getCanonicalPath}/test_cow_stream_ckpt"
      val metaClient = HoodieTableMetaClient.newTableBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      addData(tablePath, Seq(("2", "a1", "11", "001")))
      addData(tablePath, Seq(("3", "a1", "12", "002")))

      val instants = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants.getInstants
      assertEquals(3, instants.size())

      // If the request time is used, i.e., V1, then the second record is included in the output.
      // Otherwise, only third record in the output.
      val startTimestamp = instants.get(1).requestedTime
      for (streamingReadTableVersion <- List(HoodieTableVersion.SIX.versionCode(), HoodieTableVersion.EIGHT.versionCode())) {
        val df = spark.readStream
          .format("org.apache.hudi")
          .option(START_OFFSET.key, startTimestamp)
          .option(WRITE_TABLE_VERSION.key, HoodieTableVersion.current().versionCode().toString)
          .option(STREAMING_READ_TABLE_VERSION.key, streamingReadTableVersion.toString)
          .load(tablePath)
          .select("id", "name", "price", "ts")
        val expectedRows = if (streamingReadTableVersion == HoodieTableVersion.EIGHT.versionCode()) {
          Seq(Row("2", "a1", "11", "001"), Row("3", "a1", "12", "002"))
        } else {
          Seq(Row("3", "a1", "12", "002"))
        }
        testStream(df)(
          AssertOnQuery { q => q.processAllAvailable(); true },
          // Start after the first commit
          CheckAnswerRows(expectedRows, lastOnly = true, isSorted = false)
        )
      }
    }
  }

  private def addData(inputPath: String,
                      rows: Seq[(String, String, String, String)],
                      enableInlineCompaction: Boolean = false,
                      enableInlineCluster: Boolean = false) : Unit = {
    rows.toDF(columns: _*)
      .write
      .format("org.apache.hudi")
      .options(commonOptions)
      .option(TBL_NAME.key, getTableName(inputPath))
      .option(HoodieCompactionConfig.INLINE_COMPACT.key(), enableInlineCompaction.toString)
      .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "2")
      .option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), enableInlineCluster.toString)
      .option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2")
      .mode(SaveMode.Append)
      .save(inputPath)
  }

  private def addDataToQuery(inputPath: String,
                             rows: Seq[(String, String, String, String)]): AssertOnQuery = {
    AssertOnQuery { _=>
      addData(inputPath, rows)
      true
    }
  }

  private def getTableName(inputPath: String): String = {
    val start = inputPath.lastIndexOf('/')
    inputPath.substring(start + 1)
  }
}
