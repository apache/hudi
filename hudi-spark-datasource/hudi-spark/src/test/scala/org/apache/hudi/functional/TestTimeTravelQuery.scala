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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, ScalaAssertionSupport}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieCleaningPolicy, HoodieTableType}
import org.apache.hudi.common.model.HoodieTableType.{COPY_ON_WRITE, MERGE_ON_READ}
import org.apache.hudi.common.table.{HoodieTableConfig, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.TimelineUtils
import org.apache.hudi.common.testutils.{HoodieTestTable, HoodieTestUtils}
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieTimeTravelException
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.SaveMode.{Append, Overwrite}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.text.SimpleDateFormat

class TestTimeTravelQuery extends HoodieSparkClientTestBase with ScalaAssertionSupport {
  var spark: SparkSession = _
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    HoodieTableConfig.ORDERING_FIELDS.key -> "version",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach override def setUp(): Unit = {
    setTableName("hoodie_test")
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
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testTimeTravelQuery(tableType: HoodieTableType): Unit = {
    initMetaClient(tableType)
    val _spark = spark
    import _spark.implicits._

    val opts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> ""
    )

    // First write
    val df1 = Seq((1, "a1", 10, 1000)).toDF("id", "name", "value", "version")
    val firstCommit = writeBatch(df1, opts, Overwrite)

    // Second write
    val df2 = Seq((1, "a1", 12, 1001)).toDF("id", "name", "value", "version")
    val secondCommit = writeBatch(df2, opts)

    // Third write
    val df3 = Seq((1, "a1", 13, 1002)).toDF("id", "name", "value", "version")
    val thirdCommit = writeBatch(df3, opts)

    // Query as of firstCommitTime
    val result1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, firstCommit)
      .load(basePath)
      .select("id", "name", "value", "version")
      .take(1)(0)
    assertEquals(Row(1, "a1", 10, 1000), result1)

    // Query as of secondCommitTime
    val result2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, secondCommit)
      .load(basePath)
      .select("id", "name", "value", "version")
      .take(1)(0)
    assertEquals(Row(1, "a1", 12, 1001), result2)

    // Query as of thirdCommitTime
    val result3 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, thirdCommit)
      .load(basePath)
      .select("id", "name", "value", "version")
      .take(1)(0)
    assertEquals(Row(1, "a1", 13, 1002), result3)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testTimeTravelQueryWithIncompleteCommit(tableType: HoodieTableType): Unit = {
    initMetaClient(tableType)
    val _spark = spark
    import _spark.implicits._

    val opts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> ""
    )

    // First write
    val df1 = Seq((1, "a1", 10, 1000)).toDF("id", "name", "value", "version")
    val firstCommit = writeBatch(df1, opts, Overwrite)

    // Second write
    val df2 = Seq((1, "a1", 12, 1001)).toDF("id", "name", "value", "version")
    val secondCommit = writeBatch(df2, opts)

    // Third write
    val df3 = Seq((1, "a1", 13, 1002)).toDF("id", "name", "value", "version")
    val thirdCommit = writeBatch(df3, opts)

    // add an incomplete commit btw 1st and 2nd commit
    // it'll be 1 ms after 1st commit, which won't clash with 2nd commit timestamp
    val incompleteCommit = (firstCommit.toLong + 1).toString
    tableType match {
      case COPY_ON_WRITE => HoodieTestTable.of(metaClient).addInflightCommit(incompleteCommit)
      case MERGE_ON_READ => HoodieTestTable.of(metaClient).addInflightDeltaCommit(incompleteCommit)
    }

    // Query as of firstCommitTime
    val result1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, firstCommit)
      .load(basePath)
      .select("id", "name", "value", "version")
      .take(1)(0)
    assertEquals(Row(1, "a1", 10, 1000), result1)

    // Query as of other commits
    List(incompleteCommit, secondCommit, thirdCommit)
      .foreach(commitTime => {
        assertThrows(classOf[HoodieTimeTravelException]) {
          spark.read.format("hudi")
            .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, commitTime)
            .load(basePath)
            .select("id", "name", "value", "version")
            .take(1)(0)
        }
      })
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testTimeTravelQueryForPartitionedTable(tableType: HoodieTableType): Unit = {
    initMetaClient(tableType)
    val _spark = spark
    import _spark.implicits._

    val opts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      HoodieTableConfig.ORDERING_FIELDS.key -> "version",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "dt"
    )

    // First write
    val df1 = Seq((1, "a1", 10, 1000, "2021-07-26")).toDF("id", "name", "value", "version", "dt")
    val firstCommit = writeBatch(df1, opts, Overwrite)

    // Second write
    val df2 = Seq((1, "a1", 12, 1001, "2021-07-26")).toDF("id", "name", "value", "version", "dt")
    val secondCommit = writeBatch(df2, opts)

    // Third write
    val df3 = Seq((1, "a1", 13, 1002, "2021-07-26")).toDF("id", "name", "value", "version", "dt")
    val thirdCommit = writeBatch(df3, opts)

    // query as of firstCommitTime (using 'yyyy-MM-dd HH:mm:ss' format)
    val result1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, defaultDateTimeFormat(firstCommit))
      .load(basePath)
      .select("id", "name", "value", "version", "dt")
      .take(1)(0)
    assertEquals(Row(1, "a1", 10, 1000, "2021-07-26"), result1)

    // query as of secondCommitTime (using 'yyyyMMddHHmmss' format)
    val result2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, secondCommit)
      .load(basePath)
      .select("id", "name", "value", "version", "dt")
      .take(1)(0)
    assertEquals(Row(1, "a1", 12, 1001, "2021-07-26"), result2)

    // query as of thirdCommitTime
    val result3 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, thirdCommit)
      .load(basePath)
      .select("id", "name", "value", "version", "dt")
      .take(1)(0)
    assertEquals(Row(1, "a1", 13, 1002, "2021-07-26"), result3)

    // query by 'yyyy-MM-dd' format
    val result4 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, defaultDateFormat(thirdCommit))
      .load(basePath)
      .select("id", "name", "value", "version", "dt")
      .collect()
    // since there is no commit before the commit date, the query result should be empty.
    assertTrue(result4.isEmpty)
  }

  private def writeBatch(df: DataFrame, options: Map[String, String], mode: SaveMode = Append): String = {
    df.write.format("hudi").options(options).mode(mode).save(basePath)
    metaClient.reloadActiveTimeline()
    metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().requestedTime
  }

  private def defaultDateTimeFormat(queryInstant: String): String = {
    val date = TimelineUtils.parseDateFromInstantTime(queryInstant)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format.format(date)
  }

  private def defaultDateFormat(queryInstant: String): String = {
    val date = TimelineUtils.parseDateFromInstantTime(queryInstant)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.format(date)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testTimeTravelQueryWithSchemaEvolution(tableType: HoodieTableType): Unit = {
    initMetaClient(tableType)
    val _spark = spark
    import _spark.implicits._

    metaClient = createMetaClient(spark, basePath)

    val opts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "name"
    )

    // First write
    val df1 = Seq((1, "a1", 10, 1000)).toDF("id", "name", "value", "version")
    val firstCommit = writeBatch(df1, opts, Overwrite)

    // Second write
    val df2 = Seq((1, "a1", 12, 1001, "2022")).toDF("id", "name", "value", "version", "year")
    val secondCommit = writeBatch(df2, opts)

    // Third write
    val df3 = Seq((1, "a1", 13, 1002, "2022", "08")).toDF("id", "name", "value", "version", "year", "month")
    val thirdCommit = writeBatch(df3, opts)

    val tableSchemaResolver = new TableSchemaResolver(metaClient)

    // Query as of firstCommitTime
    val result1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, firstCommit)
      .load(basePath)
      .select("id", "name", "value", "version")
      .take(1)(0)
    assertEquals(Row(1, "a1", 10, 1000), result1)
    val schema1 = tableSchemaResolver.getTableAvroSchema(firstCommit)
    assertNull(schema1.getField("year"))
    assertNull(schema1.getField("month"))

    // Query as of secondCommitTime
    val result2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, secondCommit)
      .load(basePath)
      .select("id", "name", "value", "version", "year")
      .take(1)(0)
    assertEquals(Row(1, "a1", 12, 1001, "2022"), result2)
    val schema2 = tableSchemaResolver.getTableAvroSchema(secondCommit)
    assertNotNull(schema2.getField("year"))
    assertNull(schema2.getField("month"))

    // Query as of thirdCommitTime
    val result3 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, thirdCommit)
      .load(basePath)
      .select("id", "name", "value", "version", "year", "month")
      .take(1)(0)
    assertEquals(Row(1, "a1", 13, 1002, "2022", "08"), result3)
    val schema3 = tableSchemaResolver.getTableAvroSchema(thirdCommit)
    assertNotNull(schema3.getField("year"))
    assertNotNull(schema3.getField("month"))
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testTimeTravelQueryCommitsBasedClean(tableType: HoodieTableType): Unit = {
      testTimeTravelQueryCOW(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name, tableType)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testTimeTravelQueryFileVersionBasedClean(tableType: HoodieTableType): Unit = {
    testTimeTravelQueryCOW(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name, tableType)
  }

  def testTimeTravelQueryCOW(cleanerPolicy: String, tableType: HoodieTableType): Unit = {
    initMetaClient(tableType)
    val _spark = spark
    import _spark.implicits._

    val opts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "",
      HoodieCleanConfig.CLEANER_POLICY.key() -> cleanerPolicy,
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "2",
      HoodieCleanConfig.CLEANER_FILE_VERSIONS_RETAINED.key() -> "2",
      HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key() -> "3",
      HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key() -> "4",
      HoodieMetadataConfig.ENABLE.key() -> "false",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "1"
    )

    // First write
    val df1 = Seq((1, "a1", 10, 1000)).toDF("id", "name", "value", "version")
    val firstCommit = writeBatch(df1, opts, Overwrite)

    // Second write
    writeBatch(Seq((1, "a1", 12, 1001)).toDF("id", "name", "value", "version"), opts)

    // Third write
    val df3 = Seq((1, "a1", 13, 1002)).toDF("id", "name", "value", "version")
    val thirdCommit = writeBatch(df3, opts)

    // Fourth write
    writeBatch(Seq((1, "a1", 14, 1003)).toDF("id", "name", "value", "version"), opts)

    // Query as of thirdCommitTime
    val result3 = spark.read.format("hudi")
      .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, thirdCommit)
      .load(basePath)
      .select("id", "name", "value", "version")
      .take(1)(0)
    assertEquals(Row(1, "a1", 13, 1002), result3)

    if (!cleanerPolicy.equals(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name)) {
      // first commit should fail since cleaner already cleaned up.
      val e1 = assertThrows(classOf[IllegalArgumentException]) {
        spark.read.format("hudi")
          .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, firstCommit)
          .load(basePath)
          .select("id", "name", "value", "version")
          .take(1)
      }
      assertTrue(HoodieTestUtils.getRootCause(e1).getMessage.contains("Cleaner cleaned up the timestamp of interest. Please ensure sufficient commits are retained with cleaner for Timestamp as of query to work"))
    }

    // add more writes so that first commit goes into archived timeline.
    // fifth write
    writeBatch(Seq((1, "a1", 15, 1004)).toDF("id", "name", "value", "version"), opts)

    // sixth write
    writeBatch(Seq((1, "a1", 16, 1005)).toDF("id", "name", "value", "version"), opts)

    // for commits and hours based cleaning, cleaner based exception will be thrown. For file versions based cleaning,
    // archival based exception will be thrown.
    val expectedErrorMsg = if (!cleanerPolicy.equals(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name)) {
      "Cleaner cleaned up the timestamp of interest. Please ensure sufficient commits are retained with cleaner for Timestamp as of query to work"
    } else {
     "Please ensure sufficient commits are retained (uncleaned and un-archived) for timestamp as of query to work."
    }

    val e2 = assertThrows(classOf[IllegalArgumentException]) {
      spark.read.format("hudi")
        .option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, firstCommit)
        .load(basePath)
        .select("id", "name", "value", "version")
        .take(1)
    }
    assertTrue(HoodieTestUtils.getRootCause(e2).getMessage.contains(expectedErrorMsg))
  }
}
