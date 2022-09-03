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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.{ComplexKeyGenerator, NonpartitionedKeyGenerator}
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.text.SimpleDateFormat

class TestTimeTravelQuery extends HoodieClientTestBase {
  var spark: SparkSession =_
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "version",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown() = {
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

    // First write
    val df1 = Seq((1, "a1", 10, 1000)).toDF("id", "name", "value", "version")
    df1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(PARTITIONPATH_FIELD.key, "")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val firstCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

    // Second write
    val df2 = Seq((1, "a1", 12, 1001)).toDF("id", "name", "value", "version")
    df2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(PARTITIONPATH_FIELD.key, "")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Append)
      .save(basePath)
    metaClient.reloadActiveTimeline()
    val secondCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

    // Third write
    val df3 = Seq((1, "a1", 13, 1002)).toDF("id", "name", "value", "version")
    df3.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(PARTITIONPATH_FIELD.key, "")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Append)
      .save(basePath)
    metaClient.reloadActiveTimeline()
    val thirdCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

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
  def testTimeTravelQueryForPartitionedTable(tableType: HoodieTableType): Unit = {
    initMetaClient(tableType)
    val _spark = spark
    import _spark.implicits._

    // First write
    val df1 = Seq((1, "a1", 10, 1000, "2021-07-26")).toDF("id", "name", "value", "version", "dt")
    df1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "version")
      .option(PARTITIONPATH_FIELD.key, "dt")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val firstCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

    // Second write
    val df2 = Seq((1, "a1", 12, 1001, "2021-07-26")).toDF("id", "name", "value", "version", "dt")
    df2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "version")
      .option(PARTITIONPATH_FIELD.key, "dt")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
      .mode(SaveMode.Append)
      .save(basePath)
    metaClient.reloadActiveTimeline()
    val secondCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

    // Third write
    val df3 = Seq((1, "a1", 13, 1002, "2021-07-26")).toDF("id", "name", "value", "version", "dt")
    df3.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "version")
      .option(PARTITIONPATH_FIELD.key, "dt")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
      .mode(SaveMode.Append)
      .save(basePath)
    metaClient.reloadActiveTimeline()
    val thirdCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

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

  private def defaultDateTimeFormat(queryInstant: String): String = {
    val date = HoodieActiveTimeline.parseDateFromInstantTime(queryInstant)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    format.format(date)
  }

  private def defaultDateFormat(queryInstant: String): String = {
    val date = HoodieActiveTimeline.parseDateFromInstantTime(queryInstant)
    val format = new SimpleDateFormat("yyyy-MM-dd")
    format.format(date)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testTimeTravelQueryWithSchemaEvolution(tableType: HoodieTableType): Unit = {
    initMetaClient(tableType)
    val _spark = spark
    import _spark.implicits._

    // First write
    val df1 = Seq((1, "a1", 10, 1000)).toDF("id", "name", "value", "version")
    df1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(PARTITIONPATH_FIELD.key, "")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()
    val firstCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

    // Second write
    val df2 = Seq((1, "a1", 12, 1001, "2022")).toDF("id", "name", "value", "version", "year")
    df2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(PARTITIONPATH_FIELD.key, "")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Append)
      .save(basePath)
    metaClient.reloadActiveTimeline()
    val secondCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

    // Third write
    val df3 = Seq((1, "a1", 13, 1002, "2022", "08")).toDF("id", "name", "value", "version", "year", "month")
    df3.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
      .option(PARTITIONPATH_FIELD.key, "")
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[NonpartitionedKeyGenerator].getName)
      .mode(SaveMode.Append)
      .save(basePath)
    metaClient.reloadActiveTimeline()
    val thirdCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().getTimestamp

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
}
