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
import org.apache.hudi.HoodieConversionUtils.toJavaOption
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.Option
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.util.function.Consumer

class TestNestedPartitionPathSchema extends HoodieSparkClientTestBase with ScalaAssertionSupport {
  var spark: SparkSession = null

  override def getSparkSessionExtensionsInjector: Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @BeforeEach override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  def getWriterReaderOpts(recordType: HoodieRecordType, tableType: HoodieTableType): (Map[String, String], Map[String, String]) = {
    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "_meta.partition.year,_meta.partition.month,_meta.partition.day",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[ComplexKeyGenerator].getName,
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieTableConfig.TYPE.key -> tableType.name(),
      HoodieTableConfig.RECORD_MERGE_MODE.key -> recordType.name()
    )

    val writeOpts = commonOpts ++ Map(
      "hoodie.datasource.write.hive_style_partitioning" -> "true"
    )

    val readOpts = Map(
      DataSourceReadOptions.ENABLE_HOODIE_FILE_INDEX.key -> "true"
    )

    (writeOpts, readOpts)
  }

  def createNestedMetaSchema(): StructType = {
    StructType(Array(
      StructField("id", StringType, nullable = false),
      StructField("name", StringType, nullable = true),
      StructField("value", IntegerType, nullable = true),
      StructField("_meta", StructType(Array(
        StructField("partition", StructType(Array(
          StructField("year", LongType, nullable = false),
          StructField("month", LongType, nullable = false),
          StructField("day", LongType, nullable = false)
        )), nullable = false)
      )), nullable = false)
    ))
  }

  def createTestData(): DataFrame = {
    val schema = createNestedMetaSchema()
    val data = Seq(
      Row("1", "name1", 100, Row(Row(2025L, 8L, 15L))),
      Row("2", "name2", 200, Row(Row(2025L, 8L, 14L))),
      Row("3", "name3", 300, Row(Row(2023L, 1L, 10L))),
      Row("4", "name4", 400, Row(Row(2022L, 7L, 5L))),
      Row("5", "name5", 500, Row(Row(2024L, 12L, 1L)))
    )
    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testNestedPartitionPathSchemaIssue(tableType: HoodieTableType): Unit = {
    val (writeOpts, readOpts) = getWriterReaderOpts(HoodieRecordType.AVRO, tableType)
    val inputDF = createTestData()

    inputDF.write.format("hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val resultDF = spark.read.format("hudi")
      .options(readOpts)
      .load(basePath)

    val result = resultDF.collect()

    assertEquals(5, result.length)
  }

  @Test
  def testNestedPartitionPathSchemaIssueCOW(): Unit = {
    testNestedPartitionPathSchemaIssue(HoodieTableType.COPY_ON_WRITE)
  }

  @Test
  def testNestedPartitionPathSchemaIssueMOR(): Unit = {
    testNestedPartitionPathSchemaIssue(HoodieTableType.MERGE_ON_READ)
  }

}
