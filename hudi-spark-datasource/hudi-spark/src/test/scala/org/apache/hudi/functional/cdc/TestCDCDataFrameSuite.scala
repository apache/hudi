/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional.cdc

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord

import org.apache.hadoop.fs.Path

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieLogFile}
import org.apache.hudi.common.table.cdc.{HoodieCDCSupplementalLoggingMode, HoodieCDCUtils}
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.{HoodieDataBlock, HoodieLogBlock}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.RawTripTestPayload.{deleteRecordsToStrings, recordsToStrings}
import org.apache.hudi.config.{HoodieCleanConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class TestCDCDataFrameSuite extends HoodieClientTestBase {

  var spark: SparkSession = _

  val commonOpts = Map(
    HoodieTableConfig.CDC_ENABLED.key -> "true",
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    RECORDKEY_FIELD.key -> "_row_key",
    PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1",
    HoodieCleanConfig.AUTO_CLEAN.key -> "false"
  )

  @BeforeEach override def setUp(): Unit = {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initFileSystem()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @CsvSource(Array("cdc_op_key", "cdc_data_before", "cdc_data_before_after"))
  def testCOWDataSourceWrite(cdcSupplementalLoggingMode: String): Unit = {
    val options = commonOpts ++ Map(
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> cdcSupplementalLoggingMode
    )

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // init meta client
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()
    val instant1 = metaClient.reloadActiveTimeline.lastInstant().get()
    assertEquals(spark.read.format("hudi").load(basePath).count(), 100)
    // all the data is new-coming, it will write out cdc log files.
    assertFalse(hasCDCLogFile(instant1))

    val schemaResolver = new TableSchemaResolver(metaClient)
    val dataSchema = schemaResolver.getTableAvroSchema(false)
    val cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(
      HoodieCDCSupplementalLoggingMode.parse(cdcSupplementalLoggingMode), dataSchema)

    // Upsert Operation
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("001", 50)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant2 = metaClient.reloadActiveTimeline.lastInstant().get()
    assertEquals(spark.read.format("hudi").load(basePath).count(), 100)

    // part of data are updated, it will write out cdc log files
    assertTrue(hasCDCLogFile(instant2))
    val cdcData2 = getCDCLogFIle(instant2).flatMap(readCDCLogFile(_, cdcSchema))
    assertEquals(cdcData2.size, 50)

    // Delete Operation
    val records3 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .option(OPERATION.key, DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant3 = metaClient.reloadActiveTimeline.lastInstant().get()
    assertEquals(spark.read.format("hudi").load(basePath).count(), 80)

    // part of data are deleted, it will write out cdc log files
    assertTrue(hasCDCLogFile(instant3))
    val cdcData3 = getCDCLogFIle(instant3).flatMap(readCDCLogFile(_, cdcSchema))
    assertEquals(cdcData3.size, 20)
  }

  @ParameterizedTest
  @CsvSource(Array("cdc_op_key", "cdc_data_before", "cdc_data_before_after"))
  def testMORDataSourceWrite(cdcSupplementalLoggingMode: String): Unit = {
    val options = commonOpts ++ Map(
      TABLE_TYPE.key() -> MOR_TABLE_TYPE_OPT_VAL,
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> cdcSupplementalLoggingMode
    )

    // 1. Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // init meta client
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()

    val schemaResolver = new TableSchemaResolver(metaClient)
    val dataSchema = schemaResolver.getTableAvroSchema(false)
    val cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(
      HoodieCDCSupplementalLoggingMode.parse(cdcSupplementalLoggingMode), dataSchema)

    val instant1 = metaClient.reloadActiveTimeline.lastInstant().get()
    // all the data is new-coming, it will NOT write out cdc log files.
    assertFalse(hasCDCLogFile(instant1))

    // 2. Upsert Operation
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("001", 30)).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val records22 = recordsToStrings(dataGen.generateInserts("001", 20)).toList
    val inputDF22 = spark.read.json(spark.sparkContext.parallelize(records22, 2))
    inputDF2.union(inputDF22).write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant2 = metaClient.reloadActiveTimeline.lastInstant().get()

    // part of data are updated, it will write out cdc log files
    assertTrue(hasCDCLogFile(instant2))
    val cdcData2 = getCDCLogFIle(instant2).flatMap(readCDCLogFile(_, cdcSchema))
    assertEquals(cdcData2.size, 50)

    // 3. Delete Operation
    val records3 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .option(OPERATION.key, DELETE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant3 = metaClient.reloadActiveTimeline.lastInstant().get()
    // in cases that there is log files, it will NOT write out cdc log files.
    assertFalse(hasCDCLogFile(instant3))
  }

  /**
   * whether this instant will create a cdc log file.
   */
  private def hasCDCLogFile(instant: HoodieInstant): Boolean = {
    val commitMetadata = HoodieCommitMetadata.fromBytes(
      metaClient.reloadActiveTimeline().getInstantDetails(instant).get(),
      classOf[HoodieCommitMetadata]
    )
    val hoodieWriteStats = commitMetadata.getWriteStats.asScala
    hoodieWriteStats.exists { hoodieWriteStat =>
      val cdcPath = hoodieWriteStat.getCdcPath
      cdcPath != null && cdcPath.nonEmpty
    }
  }

  /**
   * whether this instant will create a cdc log file.
   */
  private def getCDCLogFIle(instant: HoodieInstant): List[String] = {
    val commitMetadata = HoodieCommitMetadata.fromBytes(
      metaClient.reloadActiveTimeline().getInstantDetails(instant).get(),
      classOf[HoodieCommitMetadata]
    )
    commitMetadata.getWriteStats.asScala.map(_.getCdcPath).toList
  }

  private def readCDCLogFile(relativeLogFile: String, cdcSchema: Schema): List[IndexedRecord] = {
    val logFile = new HoodieLogFile(
      metaClient.getFs.getFileStatus(new Path(metaClient.getBasePathV2, relativeLogFile)))
    val reader = HoodieLogFormat.newReader(fs, logFile, cdcSchema);
    assertTrue(reader.hasNext);

    val block = reader.next().asInstanceOf[HoodieDataBlock];
    block.getRecordIterator.asScala.toList
  }
}
