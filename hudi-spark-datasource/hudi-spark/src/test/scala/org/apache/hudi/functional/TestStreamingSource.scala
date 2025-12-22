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

import org.apache.hudi.DataSourceReadOptions.START_OFFSET
import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.model.HoodieTableType.{COPY_ON_WRITE, MERGE_ON_READ}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling.{BLOCK, USE_TRANSITION_TIME}
import org.apache.hudi.config.HoodieWriteConfig.{DELETE_PARALLELISM_VALUE, INSERT_PARALLELISM_VALUE, TBL_NAME, UPSERT_PARALLELISM_VALUE}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}

import org.apache.spark.sql.streaming.StreamTest
import org.apache.spark.sql.{Row, SaveMode}

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

  val handlingMode: HollowCommitHandling = BLOCK

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
      HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      val df = spark.readStream
        .format("org.apache.hudi")
        .option(DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), handlingMode.name())
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
      HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(MERGE_ON_READ)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      val df = spark.readStream
        .format("org.apache.hudi")
        .option(DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), handlingMode.name())
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
      HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      val df = spark.readStream
        .format("org.apache.hudi")
        .option(START_OFFSET.key(), "latest")
        .option(DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), handlingMode.name())
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
      val metaClient = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(COPY_ON_WRITE)
        .setTableName(getTableName(tablePath))
        .setRecordKeyFields("id")
        .setPayloadClassName(DataSourceWriteOptions.PAYLOAD_CLASS_NAME.defaultValue)
        .setPreCombineField("ts")
        .initTable(HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf()), tablePath)

      addData(tablePath, Seq(("1", "a1", "10", "000")))
      addData(tablePath, Seq(("2", "a1", "11", "001")))
      addData(tablePath, Seq(("3", "a1", "12", "002")))

      val timestamp = if (handlingMode == USE_TRANSITION_TIME) {
        metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()
          .firstInstant().get().getStateTransitionTime
      } else {
        metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants()
          .firstInstant().get().getTimestamp
      }
      val df = spark.readStream
        .format("org.apache.hudi")
        .option(START_OFFSET.key(), timestamp)
        .option(DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT.key(), handlingMode.name())
        .load(tablePath)
        .select("id", "name", "price", "ts")

      testStream(df)(
        AssertOnQuery { q => q.processAllAvailable(); true },
        // Start after the first commit
        CheckAnswerRows(Seq(Row("2", "a1", "11", "001"), Row("3", "a1", "12", "002")), lastOnly = true, isSorted = false)
      )
    }
  }

  private def addData(inputPath: String, rows: Seq[(String, String, String, String)]): Unit = {
    rows.toDF(columns: _*)
      .write
      .format("org.apache.hudi")
      .options(commonOptions)
      .option(TBL_NAME.key, getTableName(inputPath))
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
