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
import org.apache.hudi.common.model.{HoodieKey, HoodieLogFile, HoodieRecord}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.cdc.{HoodieCDCOperation, HoodieCDCSupplementalLoggingMode, HoodieCDCUtils}
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.{DATA_BEFORE, OP_KEY_ONLY}
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.HoodieDataBlock
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.testutils.RawTripTestPayload
import org.apache.hudi.config.{HoodieCleanConfig, HoodieWriteConfig}
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertNull}

import java.util.function.Predicate

import scala.collection.JavaConverters._

abstract class HoodieCDCTestBase extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  val commonOpts = Map(
    HoodieTableConfig.CDC_ENABLED.key -> "true",
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    RECORDKEY_FIELD.key -> "_row_key",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1",
    HoodieCleanConfig.AUTO_CLEAN.key -> "false",
    HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS.key -> "10"
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
    val commitMetadata = metaClient.reloadActiveTimeline.readCommitMetadata(instant)
    val hoodieWriteStats = commitMetadata.getWriteStats.asScala
    hoodieWriteStats.exists { hoodieWriteStat =>
      val cdcPaths = hoodieWriteStat.getCdcStats
      cdcPaths != null && !cdcPaths.isEmpty &&
        cdcPaths.keySet().asScala.forall(_.endsWith(HoodieCDCUtils.CDC_LOGFILE_SUFFIX))
    }
  }

  /**
   * extract a list of cdc log file.
   */
  protected def getCDCLogFile(instant: HoodieInstant): List[String] = {
    val commitMetadata = metaClient.reloadActiveTimeline().readCommitMetadata(instant)
    commitMetadata.getWriteStats.asScala.flatMap(_.getCdcStats.asScala.keys).toList
  }

  protected def isFilesExistInFileSystem(files: List[String]): Boolean = {
    files.asJava.stream().allMatch(new Predicate[String] {
      override def test(file: String): Boolean = storage.exists(new StoragePath(basePath + "/" + file))
    })
  }

  protected def getCDCBlocks(relativeLogFile: String, cdcSchema: Schema): List[HoodieDataBlock] = {
    val logFile = new HoodieLogFile(
      metaClient.getStorage.getPathInfo(new StoragePath(metaClient.getBasePath, relativeLogFile)))
    val reader = HoodieLogFormat.newReader(storage, logFile, cdcSchema)
    val blocks = scala.collection.mutable.ListBuffer.empty[HoodieDataBlock]
    while(reader.hasNext) {
      blocks.asJava.add(reader.next().asInstanceOf[HoodieDataBlock])
    }
    blocks.toList
  }

  protected def readCDCLogFile(relativeLogFile: String, cdcSchema: Schema): List[HoodieRecord[_]] = {
    val records = scala.collection.mutable.ListBuffer.empty[HoodieRecord[_]]
    val blocks = getCDCBlocks(relativeLogFile, cdcSchema)
    blocks.foreach { block =>
      records.asJava.addAll(block.getRecordIterator[IndexedRecord](HoodieRecordType.AVRO).asScala.toList.asJava)
    }
    records.toList
  }

  protected def checkCDCDataForInsertOrUpdate(loggingMode: HoodieCDCSupplementalLoggingMode,
                                              cdcSchema: Schema,
                                              dataSchema: Schema,
                                              cdcRecords: Seq[HoodieRecord[_]],
                                              newHoodieRecords: java.util.List[HoodieRecord[_]],
                                              op: HoodieCDCOperation): Unit = {
    val cdcRecord = cdcRecords.head.getData.asInstanceOf[GenericRecord]
    // check schema
    assertEquals(cdcRecord.getSchema, cdcSchema)
    if (loggingMode == OP_KEY_ONLY) {
      // check record key
      assert(cdcRecords.map(_.getData.asInstanceOf[GenericRecord].get(1).toString).sorted == newHoodieRecords.asScala.map(_.getKey.getRecordKey).sorted)
    } else if (loggingMode == DATA_BEFORE) {
      // check record key
      assert(cdcRecords.map(_.getData.asInstanceOf[GenericRecord].get(1).toString).sorted == newHoodieRecords.asScala.map(_.getKey.getRecordKey).sorted)
      // check before
      if (op == HoodieCDCOperation.INSERT) {
        assertNull(cdcRecord.get("before"))
      } else {
        val payload = newHoodieRecords.asScala.find(_.getKey.getRecordKey == cdcRecord.get("record_key").toString).get
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
        val payload = newHoodieRecords.asScala.find(_.getKey.getRecordKey == cdcAfterValue.get("_row_key").toString).get
          .getData.asInstanceOf[RawTripTestPayload]
        val genericRecord = payload.getInsertValue(dataSchema).get.asInstanceOf[GenericRecord]
        assertEquals(genericRecord.get("begin_lat"), cdcAfterValue.get("begin_lat"))
      } else {
        val payload = newHoodieRecords.asScala.find(_.getKey.getRecordKey == cdcAfterValue.get("_row_key").toString).get
          .getData.asInstanceOf[RawTripTestPayload]
        val genericRecord = payload.getInsertValue(dataSchema).get.asInstanceOf[GenericRecord]
        // check before
        assertNotEquals(genericRecord.get("begin_lat"), cdcBeforeValue.get("begin_lat"))
        // check after
        assertEquals(genericRecord.get("begin_lat"), cdcAfterValue.get("begin_lat"))
      }
    }
  }

  protected def checkCDCDataForDelete(loggingMode: HoodieCDCSupplementalLoggingMode,
                                      cdcSchema: Schema,
                                      cdcRecords: Seq[IndexedRecord],
                                      deletedKeys: java.util.List[HoodieKey]): Unit = {
    val cdcRecord = cdcRecords.head.asInstanceOf[GenericRecord]
    // check schema
    assertEquals(cdcRecord.getSchema, cdcSchema)
    if (loggingMode == OP_KEY_ONLY) {
      // check record key
      assert(cdcRecords.map(_.get(1).toString).sorted == deletedKeys.asScala.map(_.getRecordKey).sorted)
    } else if (loggingMode == DATA_BEFORE) {
      // check record key
      assert(cdcRecords.map(_.get(1).toString).sorted == deletedKeys.asScala.map(_.getRecordKey).sorted)
    } else {
      val cdcBeforeValue = cdcRecord.get("before").asInstanceOf[GenericRecord]
      val cdcAfterValue = cdcRecord.get("after").asInstanceOf[GenericRecord]
      // check before
      assert(deletedKeys.asScala.exists(_.getRecordKey == cdcBeforeValue.get("_row_key").toString))
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
