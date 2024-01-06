/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkRecordMerger, SparkDatasetMixin}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Test cases on multiple base file format support for COW and MOR table types.
 */
class TestHoodieMultipleBaseFileFormat extends HoodieSparkClientTestBase with SparkDatasetMixin {

  var spark: SparkSession = null
  private val log = LoggerFactory.getLogger(classOf[TestMORDataSource])
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieTableConfig.MULTIPLE_BASE_FILE_FORMATS_ENABLE.key -> "true",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )
  val sparkOpts = Map(
    HoodieWriteConfig.RECORD_MERGER_IMPLS.key -> classOf[HoodieSparkRecordMerger].getName,
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

  @Test
  def testMultiFileFormatForCOWTableType(): Unit = {
    insertAndValidateSnapshot(basePath, HoodieTableType.COPY_ON_WRITE.name())
  }

  @Test
  def testMultiFileFormatForMORTableType(): Unit = {
    insertAndValidateSnapshot(basePath, HoodieTableType.MERGE_ON_READ.name())
  }

  def insertAndValidateSnapshot(basePath: String, tableType: String): Unit = {
    // Insert records in Parquet format to one of the partitions.
    val records1 = recordsToStrings(dataGen.generateInsertsForPartition("001", 10, DEFAULT_FIRST_PARTITION_PATH)).asScala
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // Insert records to a new partition in ORC format.
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("002", 10, DEFAULT_SECOND_PARTITION_PATH)).asScala
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
      .option(HoodieTableConfig.BASE_FILE_FORMAT.key, HoodieFileFormat.ORC.name())
      .mode(SaveMode.Append)
      .save(basePath)

    // Snapshot Read the table
    val hudiDf = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(0, hudiDf.count())

    // Update and generate new slice across partitions.
    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("003", 10)).asScala
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
      .mode(SaveMode.Append)
      .save(basePath)

    // Snapshot Read the table
    val hudiDfAfterUpdate = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(0, hudiDfAfterUpdate.count())
  }
}
