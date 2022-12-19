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


import org.apache.hudi.HoodieConversionUtils.toJavaOption

import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieClientTestBase}
import org.apache.hudi.util.JFunction
import org.apache.hudi.{DataSourceReadOptions, DataSourceUtils, DataSourceWriteOptions, HoodieDataSourceHelpers, HoodieSparkRecordMerger, SparkDatasetMixin}
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hudi.HoodieSparkSessionExtension
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}

import java.util.function.Consumer
import scala.collection.JavaConverters._

class TestMORDataSourceWithParquetLog extends HoodieClientTestBase {

  var spark: SparkSession = null
  private val log = LogManager.getLogger(classOf[TestMORDataSource])
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  val sparkOpts = Map(
    HoodieWriteConfig.MERGER_IMPLS.key -> classOf[HoodieSparkRecordMerger].getName,
    HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

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

  override def getSparkSessionExtensionsInjector: util.Option[Consumer[SparkSessionExtensions]] =
    toJavaOption(
      Some(
        JFunction.toJavaConsumer((receiver: SparkSessionExtensions) => new HoodieSparkSessionExtension().apply(receiver)))
    )

  @ParameterizedTest
  @CsvSource(Array(
    "AVRO, AVRO, END_MAP",
    "AVRO, SPARK, END_MAP",
    "SPARK, AVRO, END_MAP",
    "AVRO, AVRO, END_ARRAY",
    "AVRO, SPARK, END_ARRAY",
    "SPARK, AVRO, END_ARRAY"))
  def testRecordTypeCompatibilityWithParquetLog(readType: HoodieRecordType,
                                                writeType: HoodieRecordType,
                                                transformMode: String): Unit = {
    var (_, readOpts) = getOpts(readType)
    var (writeOpts, _) = getOpts(writeType)
    readOpts = readOpts ++ Map(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet")
    writeOpts = writeOpts ++ Map(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet")
    val records = dataGen.generateInserts("001", 10)

    // End with array
    val inputDF1 = transfrom(spark.read.json(
      spark.sparkContext.parallelize(recordsToStrings(records).asScala, 2))
      .withColumn("wk_tenant_id", lit("wk_tenant_id"))
      .withColumn("ref_id", lit("wk_tenant_id")), transformMode)
    inputDF1.write.format("org.apache.hudi")
      .options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      // CanIndexLogFile=true
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, IndexType.INMEMORY.name())
      .mode(SaveMode.Overwrite)
      .save(basePath)
    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    val snapshotDF1 = spark.read.format("org.apache.hudi")
      .options(readOpts)
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .load(basePath + "/*/*/*/*")
    snapshotDF1.show()
    assertEquals(10, snapshotDF1.count())
  }

  def transfrom(df: DataFrame, mode: String): DataFrame = {
    mode match {
      case "END_MAP" => df
        .withColumn("obj_ids", array(lit("wk_tenant_id")))
        .withColumn("obj_maps", map(lit("wk_tenant_id"), col("obj_ids")))
      case "END_ARRAY" => df
        .withColumn("obj_maps", map(lit("wk_tenant_id"), lit("wk_tenant_id")))
        .withColumn("obj_ids", array(col("obj_maps")))
    }
  }

  def getOpts(recordType: HoodieRecordType): (Map[String, String], Map[String, String]) = {
    recordType match {
      case HoodieRecordType.SPARK => (commonOpts ++ sparkOpts, sparkOpts)
      case _ => (commonOpts, Map.empty[String, String])
    }
  }
}
