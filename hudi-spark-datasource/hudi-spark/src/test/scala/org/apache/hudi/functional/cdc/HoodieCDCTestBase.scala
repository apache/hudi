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

package org.apache.hudi.functional.cdc

import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieKey, HoodieLogFile, HoodieRecord}
import org.apache.hudi.common.table.cdc.HoodieCDCOperation
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.HoodieDataBlock
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.RawTripTestPayload
import org.apache.hudi.config.{HoodieCleanConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieClientTestBase

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNull, assertTrue}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

abstract class HoodieCDCTestBase extends HoodieClientTestBase {

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

  protected def cdcDataFrame(basePath: String, startingInstant: String, endingInstant: String): DataFrame = {
    val reader = spark.read.format("hudi")
      .option(QUERY_TYPE.key, QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(INCREMENTAL_FORMAT.key, INCREMENTAL_FORMAT_CDC_VAL)
      .option("hoodie.datasource.read.begin.instanttime", startingInstant)
    if (endingInstant != null) {
      reader.option("hoodie.datasource.read.end.instanttime", endingInstant)
    }
    reader.load(basePath)
  }

  protected def cdcDataFrame(startingInstant: String, endingInstant: String = null): DataFrame = {
    cdcDataFrame(basePath, startingInstant, endingInstant)
  }

  /**
   * whether this instant will create a cdc log file.
   */
  protected def hasCDCLogFile(instant: HoodieInstant): Boolean = {
    val commitMetadata = HoodieCommitMetadata.fromBytes(
      metaClient.reloadActiveTimeline().getInstantDetails(instant).get(),
      classOf[HoodieCommitMetadata]
    )
    val hoodieWriteStats = commitMetadata.getWriteStats.asScala
    hoodieWriteStats.exists { hoodieWriteStat =>
      val cdcPaths = hoodieWriteStat.getCdcPaths
      cdcPaths != null && cdcPaths.nonEmpty
    }
  }

  /**
   * extract a list of cdc log file.
   */
  protected def getCDCLogFile(instant: HoodieInstant): List[String] = {
    val commitMetadata = HoodieCommitMetadata.fromBytes(
      metaClient.reloadActiveTimeline().getInstantDetails(instant).get(),
      classOf[HoodieCommitMetadata]
    )
    commitMetadata.getWriteStats.asScala.flatMap(_.getCdcPaths).toList
  }

  protected def getCDCBlocks(relativeLogFile: String, cdcSchema: Schema): List[HoodieDataBlock] = {
    val logFile = new HoodieLogFile(
      metaClient.getFs.getFileStatus(new Path(metaClient.getBasePathV2, relativeLogFile)))
    val reader = HoodieLogFormat.newReader(fs, logFile, cdcSchema)
    val blocks = scala.collection.mutable.ListBuffer.empty[HoodieDataBlock]
    while(reader.hasNext) {
      blocks.add(reader.next().asInstanceOf[HoodieDataBlock])
    }
    blocks.toList
  }

  protected def readCDCLogFile(relativeLogFile: String, cdcSchema: Schema): List[IndexedRecord] = {
    val records = scala.collection.mutable.ListBuffer.empty[IndexedRecord]
    val blocks = getCDCBlocks(relativeLogFile, cdcSchema)
    blocks.foreach { block =>
      records.addAll(block.getRecordIterator.asScala.toList)
    }
    records.toList
  }

  protected def checkCDCDataForInsertOrUpdate(cdcSupplementalLoggingMode: String,
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

  protected def checkCDCDataForDelete(cdcSupplementalLoggingMode: String,
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

  protected def assertCDCOpCnt(cdcData: DataFrame, expectedInsertCnt: Long,
                               expectedUpdateCnt: Long, expectedDeletedCnt: Long): Unit = {
    assertEquals(expectedInsertCnt, cdcData.where("op = 'i'").count())
    assertEquals(expectedUpdateCnt, cdcData.where("op = 'u'").count())
    assertEquals(expectedDeletedCnt, cdcData.where("op = 'd'").count())
  }
}
