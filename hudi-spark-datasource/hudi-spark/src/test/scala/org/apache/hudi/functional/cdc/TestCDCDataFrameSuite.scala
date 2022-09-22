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
import org.apache.avro.generic.{GenericRecord, IndexedRecord}

import org.apache.hadoop.fs.Path

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieKey, HoodieLogFile, HoodieRecord}
import org.apache.hudi.common.table.cdc.{HoodieCDCOperation, HoodieCDCSupplementalLoggingMode, HoodieCDCUtils}
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.{HoodieDataBlock}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.RawTripTestPayload
import org.apache.hudi.common.testutils.RawTripTestPayload.{deleteRecordsToStrings, recordsToStrings}
import org.apache.hudi.config.{HoodieCleanConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertNull, assertTrue}
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
    val hoodieRecords2 = dataGen.generateUniqueUpdates("001", 50)
    val records2 = recordsToStrings(hoodieRecords2).toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant2 = metaClient.reloadActiveTimeline.lastInstant().get()
    assertEquals(spark.read.format("hudi").load(basePath).count(), 100)

    // part of data are updated, it will write out cdc log files
    assertTrue(hasCDCLogFile(instant2))
    val cdcData2: Seq[IndexedRecord] = getCDCLogFIle(instant2).flatMap(readCDCLogFile(_, cdcSchema))
    // check the num of cdc data
    assertEquals(cdcData2.size, 50)
    // check op
    assert(cdcData2.forall( r => r.get(0).toString == "u"))
    // check record key, before, after according to the supplemental logging mode
    checkCDCDataForInsertOrUpdate(cdcSupplementalLoggingMode, cdcSchema, dataSchema,
      cdcData2, hoodieRecords2, HoodieCDCOperation.UPDATE)

    // Delete Operation
    val hoodieKey3 = dataGen.generateUniqueDeletes(20)
    val records3 = deleteRecordsToStrings(hoodieKey3).toList
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
    // check the num of cdc data
    assertEquals(cdcData3.size, 20)
    // check op
    assert(cdcData3.forall( r => r.get(0).toString == "d"))
    // check record key, before, after according to the supplemental logging mode
    checkCDCDataForDelete(cdcSupplementalLoggingMode, cdcSchema, cdcData3, hoodieKey3)
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
    val records2_1 = recordsToStrings(dataGen.generateUniqueUpdates("001", 30)).toList
    val inputDF2_1 = spark.read.json(spark.sparkContext.parallelize(records2_1, 2))
    val records2_2 = recordsToStrings(dataGen.generateInserts("001", 20)).toList
    val inputDF2_2 = spark.read.json(spark.sparkContext.parallelize(records2_2, 2))
    inputDF2_1.union(inputDF2_2).write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant2 = metaClient.reloadActiveTimeline.lastInstant().get()

    // part of data are updated, it will write out cdc log files
    assertTrue(hasCDCLogFile(instant2))
    val cdcData2 = getCDCLogFIle(instant2).flatMap(readCDCLogFile(_, cdcSchema))
    assertEquals(cdcData2.size, 50)
    // check op
    assertEquals(cdcData2.count(r => r.get(0).toString == "u"), 30)
    assertEquals(cdcData2.count(r => r.get(0).toString == "i"), 20)

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

  private def checkCDCDataForInsertOrUpdate(cdcSupplementalLoggingMode: String,
                                            cdcSchema: Schema,
                                            dataSchema: Schema,
                                            cdcRecords: Seq[IndexedRecord],
                                            newHoodieRecords: java.util.List[HoodieRecord[_]],
                                            op: HoodieCDCOperation): Unit = {
    val cdcRecord = cdcRecords.head.asInstanceOf[GenericRecord]
    // check schema
    assertEquals(cdcRecord.getSchema, cdcSchema)
    if (cdcSupplementalLoggingMode == "cdc_op_key") {
      // check record key
      assert(cdcRecords.map(_.get(1).toString).sorted == newHoodieRecords.map(_.getKey.getRecordKey).sorted)
    } else if (cdcSupplementalLoggingMode == "cdc_data_before") {
      // check record key
      assert(cdcRecords.map(_.get(1).toString).sorted == newHoodieRecords.map(_.getKey.getRecordKey).sorted)
      // check before
      if (op == HoodieCDCOperation.INSERT) {
        assertNull(cdcRecord.get("before"))
      } else {
        val payload = newHoodieRecords.find(_.getKey.getRecordKey == cdcRecord.get("record_key").toString).get
          .getData.asInstanceOf[RawTripTestPayload]
        val genericRecord = payload.getInsertValue(dataSchema).get.asInstanceOf[GenericRecord]
        val cdcBeforeValue = cdcRecord.get("before").asInstanceOf[GenericRecord]
        assertNotEquals(genericRecord.get("begin_lat"), cdcBeforeValue.get("begin_lat"))
      }
    } else {
      val cdcBeforeValue = cdcRecord.get("before").asInstanceOf[GenericRecord]
      val cdcAfterValue = cdcRecord.get("after").asInstanceOf[GenericRecord]
      if (op == HoodieCDCOperation.INSERT) {
        // check before
        assertNull(cdcBeforeValue)
        // check after
        val payload = newHoodieRecords.find(_.getKey.getRecordKey == cdcAfterValue.get("_row_key").toString).get
          .getData.asInstanceOf[RawTripTestPayload]
        val genericRecord = payload.getInsertValue(dataSchema).get.asInstanceOf[GenericRecord]
        assertEquals(genericRecord.get("begin_lat"), cdcAfterValue.get("begin_lat"))
      } else {
        val payload = newHoodieRecords.find(_.getKey.getRecordKey == cdcAfterValue.get("_row_key").toString).get
          .getData.asInstanceOf[RawTripTestPayload]
        val genericRecord = payload.getInsertValue(dataSchema).get.asInstanceOf[GenericRecord]
        // check before
        assertNotEquals(genericRecord.get("begin_lat"), cdcBeforeValue.get("begin_lat"))
        // check after
        assertEquals(genericRecord.get("begin_lat"), cdcAfterValue.get("begin_lat"))
      }
    }
  }

  private def checkCDCDataForDelete(cdcSupplementalLoggingMode: String,
                                    cdcSchema: Schema,
                                    cdcRecords: Seq[IndexedRecord],
                                    deletedKeys: java.util.List[HoodieKey]): Unit = {
    val cdcRecord = cdcRecords.head.asInstanceOf[GenericRecord]
    // check schema
    assertEquals(cdcRecord.getSchema, cdcSchema)
    if (cdcSupplementalLoggingMode == "cdc_op_key") {
      // check record key
      assert(cdcRecords.map(_.get(1).toString).sorted == deletedKeys.map(_.getRecordKey).sorted)
    } else if (cdcSupplementalLoggingMode == "cdc_data_before") {
      // check record key
      assert(cdcRecords.map(_.get(1).toString).sorted == deletedKeys.map(_.getRecordKey).sorted)
    } else {
      val cdcBeforeValue = cdcRecord.get("before").asInstanceOf[GenericRecord]
      val cdcAfterValue = cdcRecord.get("after").asInstanceOf[GenericRecord]
      // check before
      assert(deletedKeys.exists(_.getRecordKey == cdcBeforeValue.get("_row_key").toString))
      // check after
      assertNull(cdcAfterValue)
    }
  }
}
