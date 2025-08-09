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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{MOR_TABLE_TYPE_OPT_VAL, PARTITIONPATH_FIELD_OPT_KEY, PRECOMBINE_FIELD_OPT_KEY, RECORDKEY_FIELD_OPT_KEY}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.table.{HoodieTableConfig, TableSchemaResolver}
import org.apache.hudi.common.table.cdc.{HoodieCDCOperation, HoodieCDCSupplementalLoggingMode}
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY
import org.apache.hudi.common.table.cdc.HoodieCDCUtils.schemaBySupplementalLoggingMode
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.{deleteRecordsToStrings, recordsToStrings}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException

import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource}

import scala.collection.JavaConverters._

class TestCDCDataFrameSuite extends HoodieCDCTestBase {

  /**
   * Step1: Insert 100
   * Step2: Upsert 50
   * Step3: Delete 20 With Clustering
   * Step4: Insert Overwrite 50
   * Step5: Insert 7
   * Step6: Insert 3
   * Step7: Upsert 30 With Clean
   * Step8: Bulk_Insert 20
   */
  @ParameterizedTest
  @CsvSource(Array("OP_KEY_ONLY,org.apache.hudi.io.HoodieWriteMergeHandle",
    "DATA_BEFORE,org.apache.hudi.io.HoodieWriteMergeHandle",
    "DATA_BEFORE_AFTER,org.apache.hudi.io.HoodieWriteMergeHandle",
    "OP_KEY_ONLY,org.apache.hudi.io.FileGroupReaderBasedMergeHandle",
    "DATA_BEFORE,org.apache.hudi.io.FileGroupReaderBasedMergeHandle",
    "DATA_BEFORE_AFTER,org.apache.hudi.io.FileGroupReaderBasedMergeHandle"))
  def testCOWDataSourceWrite(loggingMode: HoodieCDCSupplementalLoggingMode, mergeHandleClassName: String): Unit = {
    val options = commonOpts ++ Map(
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> loggingMode.name(),
      HoodieWriteConfig.MERGE_HANDLE_CLASS_NAME.key() -> mergeHandleClassName
    )

    var totalInsertedCnt = 0L
    var totalUpdatedCnt = 0L
    var totalDeletedCnt = 0L
    var allVisibleCDCData = spark.emptyDataFrame

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = createMetaClient(spark, basePath)

    val schemaResolver = new TableSchemaResolver(metaClient)
    val dataSchema = schemaResolver.getTableAvroSchema(false)
    val cdcSchema = schemaBySupplementalLoggingMode(loggingMode, dataSchema)

    totalInsertedCnt += 100
    val instant1 = metaClient.reloadActiveTimeline.lastInstant().get()
    // all the data is new-coming, it will write out cdc log files.
    assertFalse(hasCDCLogFile(instant1))
    val commitTime1 = instant1.requestedTime
    val cdcDataOnly1 = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly1, 100, 0, 0)

    // Upsert Operation
    val hoodieRecords2 = dataGen.generateUniqueUpdates("001", 50)
    val records2 = recordsToStrings(hoodieRecords2).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant2 = metaClient.reloadActiveTimeline.lastInstant().get()

    // part of data are updated, it will write out cdc log files
    assertTrue(hasCDCLogFile(instant2))
    // check cdc data
    val cdcDataFromCDCLogFile2 = getCDCLogFile(instant2).flatMap(readCDCLogFile(_, cdcSchema))
    // check the num of cdc data
    assertEquals(cdcDataFromCDCLogFile2.size, 50)
    // check record key, before, after according to the supplemental logging mode
    checkCDCDataForInsertOrUpdate(loggingMode, cdcSchema, dataSchema,
      cdcDataFromCDCLogFile2, hoodieRecords2, HoodieCDCOperation.UPDATE)

    val commitTime2 = instant2.requestedTime
    var currentSnapshotData = spark.read.format("hudi").load(basePath)
    // at the last commit, 100 records are inserted.
    val insertedCnt2 = currentSnapshotData.count() - 100
    val updatedCnt2 = 50 - insertedCnt2
    val cdcDataOnly2 = cdcDataFrame((commitTime2.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly2, insertedCnt2, updatedCnt2, 0)
    totalUpdatedCnt += updatedCnt2
    totalInsertedCnt += insertedCnt2

    // Delete Operation With Clustering Operation
    val records3 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).asScala.toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "1")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant3 = metaClient.reloadActiveTimeline.lastInstant().get()
    // only part of data are deleted and some data will write back to the file.
    // it will write out cdc log files. But instant3 is the clustering instant, not the delete one. so we omit to test.
    val commitTime3 = instant3.requestedTime
    currentSnapshotData = spark.read.format("hudi").load(basePath)
    // here we use `commitTime2` to query the change data in commit 3.
    // because `commitTime3` is the ts of the clustering operation, not the delete operation.
    val cdcDataOnly3 = cdcDataFrame(commitTime2)
    assertCDCOpCnt(cdcDataOnly3, 0, 0, 20)
    totalDeletedCnt += 20

    // all the change data  in the range [commitTime1, commitTime3]
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // check both starting and ending are provided
    val cdcDataFrom2To3 = cdcDataFrame(commitTime1, commitTime3)
    assertCDCOpCnt(cdcDataFrom2To3, insertedCnt2, updatedCnt2, 20)

    // Insert Overwrite Operation
    val records4 = recordsToStrings(dataGen.generateInserts("003", 50)).asScala.toList
    val inputDF4 = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant4 = metaClient.reloadActiveTimeline.lastInstant().get()
    // the files which keep all the old data will be replaced directly.
    // and all the new data will write out some new file groups.
    // it will NOT write out cdc log files
    assertFalse(hasCDCLogFile(instant4))
    val commitTime4 = instant4.requestedTime
    val cdcDataOnly4 = cdcDataFrame((commitTime4.toLong - 1).toString)
    val insertedCnt4 = 50
    val deletedCnt4 = currentSnapshotData.count()
    assertCDCOpCnt(cdcDataOnly4, insertedCnt4, 0, deletedCnt4)
    totalInsertedCnt += insertedCnt4
    totalDeletedCnt += deletedCnt4
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    val records5 = recordsToStrings(dataGen.generateInserts("005", 7)).asScala.toList
    val inputDF5 = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    val records6 = recordsToStrings(dataGen.generateInserts("006", 3)).asScala.toList
    val inputDF6 = spark.read.json(spark.sparkContext.parallelize(records6, 2))
    inputDF6.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    // Upsert Operation With Clean Operation
    val records7 = recordsToStrings(dataGen.generateUniqueUpdates("007", 30)).asScala.toList
    val inputDF7 = spark.read.json(spark.sparkContext.parallelize(records7, 2))
    inputDF7.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.clean.automatic", "true")
      .option("hoodie.keep.min.commits", "4")
      .option("hoodie.keep.max.commits", "5")
      .option("hoodie.clean.commits.retained", "3")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant7 = metaClient.reloadActiveTimeline.getCommitsTimeline.lastInstant().get()
    // part of data are updated, it will write out cdc log files.
    val cdcDataOnly7 = cdcDataFrame((instant7.requestedTime.toLong - 1).toString)
    val currentData = spark.read.format("hudi").load(basePath)
    val insertedCnt7 = currentData.count() - 60
    val updatedCnt7 = 30 - insertedCnt7
    assertCDCOpCnt(cdcDataOnly7, insertedCnt7, updatedCnt7, 0)
    // here cause we do the clean operation and just remain the commit4 to commit7,
    // so we need to reset the total cnt.
    // 60 is the number of inserted records since commit 4.
    totalInsertedCnt = 60 + insertedCnt7
    totalUpdatedCnt = updatedCnt7
    totalDeletedCnt = 0
    allVisibleCDCData = cdcDataFrame((commitTime4.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // Bulk_Insert Operation With Clean Operation
    val records8 = recordsToStrings(dataGen.generateInserts("008", 20)).asScala.toList
    val inputDF8 = spark.read.json(spark.sparkContext.parallelize(records8, 2))
    inputDF8.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant8 = metaClient.reloadActiveTimeline.lastInstant().get()
    // the files which keep all the old data will be replaced directly.
    // and all the new data will write out some new file groups.
    // it will NOT write out cdc log files
    assertFalse(hasCDCLogFile(instant8))
    val commitTime8 = instant8.requestedTime
    val cdcDataOnly8 = cdcDataFrame((commitTime8.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly8, 20, 0, 0)
    totalInsertedCnt += 20
    allVisibleCDCData = cdcDataFrame((commitTime4.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // test start commit time in archived timeline. cdc query should fail
    assertThrows(classOf[HoodieException], () => {
      cdcDataFrame((commitTime1.toLong - 1).toString)
    })
  }

  /**
   * Step1: Insert 100
   * Step2: Upsert 50
   * Step3: Delete 20 With Compaction
   * Step4: Bulk_Insert 100
   * Step5: Upsert 60 With Clustering
   * Step6: Insert Overwrite 70
   * Step7,8: Insert 10 in two commits
   * Step9: Upsert 30 With Clean
   */
  @ParameterizedTest
  @EnumSource(classOf[HoodieCDCSupplementalLoggingMode])
  def testMORDataSourceWrite(loggingMode: HoodieCDCSupplementalLoggingMode): Unit = {
    val options = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key() -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> loggingMode.name()
    )

    var totalInsertedCnt = 0L
    var totalUpdatedCnt = 0L
    var totalDeletedCnt = 0L
    var allVisibleCDCData = spark.emptyDataFrame

    // 1. Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = createMetaClient(spark, basePath)

    val schemaResolver = new TableSchemaResolver(metaClient)
    val dataSchema = schemaResolver.getTableAvroSchema(false)
    val cdcSchema = schemaBySupplementalLoggingMode(loggingMode, dataSchema)

    totalInsertedCnt += 100
    val instant1 = metaClient.reloadActiveTimeline.lastInstant().get()
    // all the data is new-coming, it will write out cdc log files.
    assertFalse(hasCDCLogFile(instant1))
    val commitTime1 = instant1.requestedTime
    val cdcDataOnly1 = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly1, 100, 0, 0)

    // 2. Upsert Operation
    val records2_1 = recordsToStrings(dataGen.generateUniqueUpdates("001", 30)).asScala.toList
    val inputDF2_1 = spark.read.json(spark.sparkContext.parallelize(records2_1, 2))
    val records2_2 = recordsToStrings(dataGen.generateInserts("001", 20)).asScala.toList
    val inputDF2_2 = spark.read.json(spark.sparkContext.parallelize(records2_2, 2))
    inputDF2_1.union(inputDF2_2).write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant2 = metaClient.reloadActiveTimeline.lastInstant().get()

    // part of data are updated, it will write out cdc log files
    assertTrue(hasCDCLogFile(instant2))
    val cdcDataFromCDCLogFile2 = getCDCLogFile(instant2).flatMap(readCDCLogFile(_, cdcSchema))
    assertEquals(cdcDataFromCDCLogFile2.size, 50)
    // check op
    assertEquals(cdcDataFromCDCLogFile2.count(r => r.getData.asInstanceOf[GenericRecord].get(0).toString == "u"), 30)
    assertEquals(cdcDataFromCDCLogFile2.count(r => r.getData.asInstanceOf[GenericRecord].get(0).toString == "i"), 20)

    val commitTime2 = instant2.requestedTime
    var currentSnapshotData = spark.read.format("hudi").load(basePath)
    // at the last commit, 100 records are inserted.
    val insertedCnt2 = currentSnapshotData.count() - 100
    val updatedCnt2 = 50 - insertedCnt2
    val cdcDataOnly2 = cdcDataFrame((commitTime2.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly2, insertedCnt2, updatedCnt2, 0)
    totalUpdatedCnt += updatedCnt2
    totalInsertedCnt += insertedCnt2

    // 3. Delete Operation With Compaction Operation
    val records3 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).asScala.toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL)
      .option("hoodie.compact.inline", "true")
      .option("hoodie.compact.inline.max.delta.commits", "1")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant3 = metaClient.reloadActiveTimeline.lastInstant().get()
    // in cases that there is log files, it will NOT write out cdc log files.
    // But instant3 is the compaction instant, not the delete one. so we omit to test.
    val commitTime3 = instant3.requestedTime
    currentSnapshotData = spark.read.format("hudi").load(basePath)
    // here we use `commitTime2` to query the change data in commit 3.
    // because `commitTime3` is the ts of the clustering operation, not the delete operation.
    val cdcDataOnly3 = cdcDataFrame(commitTime2)
    assertCDCOpCnt(cdcDataOnly3, 0, 0, 20)

    totalDeletedCnt += 20
    // all the change data  in the range [commitTime1, commitTime3]
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // 4. Bulk_Insert Operation
    val records4 = recordsToStrings(dataGen.generateInserts("003", 100)).asScala.toList
    val inputDF4 = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.compact.inline", "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant4 = metaClient.reloadActiveTimeline.lastInstant().get()
    // all the new data will write out some new file groups.
    // it will NOT write out cdc log files
    assertFalse(hasCDCLogFile(instant4))
    val commitTime4 = instant4.requestedTime
    val cntForInstant4 = spark.read.format("hudi").load(basePath).count()
    val cdcDataOnly4 = cdcDataFrame((commitTime4.toLong - 1).toString)
    val insertedCnt4 = 100
    assertCDCOpCnt(cdcDataOnly4, insertedCnt4, 0, 0)

    totalInsertedCnt += insertedCnt4
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // 5. Upsert Operation With Clustering Operation
    val records5 = recordsToStrings(dataGen.generateUniqueUpdates("004", 60)).asScala.toList
    val inputDF5 = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "1")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant5 = metaClient.reloadActiveTimeline.lastInstant().get()
    // in cases that there is log files, it will NOT write out cdc log files.
    // But instant9 is the clustering instant, not the upsert one. so we omit to test.
    val commitTime5 = instant5.requestedTime
    // here we use `commitTime4` to query the change data in commit 5.
    // because `commitTime5` is the ts of the clean operation, not the upsert operation.
    val cdcDataOnly5 = cdcDataFrame(commitTime4)
    val cntForInstant5 = spark.read.format("hudi").load(basePath).count()
    val insertedCnt5 = cntForInstant5 - cntForInstant4
    val updatedCnt5 = 60 - insertedCnt5
    assertCDCOpCnt(cdcDataOnly5, insertedCnt5, updatedCnt5, 0)

    totalInsertedCnt += insertedCnt5
    totalUpdatedCnt += updatedCnt5
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // check both starting and ending are provided
    val cdcDataFrom3To4 = cdcDataFrame(commitTime2, commitTime4)
    assertCDCOpCnt(cdcDataFrom3To4, insertedCnt4, 0, 20)

    // 6. Insert Overwrite Operation
    val records6 = recordsToStrings(dataGen.generateInserts("005", 70)).asScala.toList
    val inputDF6 = spark.read.json(spark.sparkContext.parallelize(records6, 2))
    inputDF6.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.compact.inline", "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant6 = metaClient.reloadActiveTimeline.lastInstant().get()
    // the files which keep all the old data will be replaced directly.
    // and all the new data will write out some new file groups.
    // it will NOT write out cdc log files
    assertFalse(hasCDCLogFile(instant6))
    val commitTime6 = instant6.requestedTime
    val cntForInstant6 = spark.read.format("hudi").load(basePath).count()
    val cdcDataOnly6 = cdcDataFrame((commitTime6.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly6, 70, 0, cntForInstant5)
    totalInsertedCnt += 70
    totalDeletedCnt += cntForInstant5
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // 7,8. insert 10 records
    val records7 = recordsToStrings(dataGen.generateInserts("006", 7)).asScala.toList
    val inputDF7 = spark.read.json(spark.sparkContext.parallelize(records7, 2))
    inputDF7.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Append)
      .save(basePath)
    totalInsertedCnt += 7

    val records8 = recordsToStrings(dataGen.generateInserts("007", 3)).asScala.toList
    val inputDF8 = spark.read.json(spark.sparkContext.parallelize(records8, 2))
    inputDF8.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant8 = metaClient.reloadActiveTimeline.lastInstant().get()
    val commitTime8 = instant8.requestedTime
    totalInsertedCnt += 3

    // 8. Upsert Operation With Clean Operation
    val inputDF9 = inputDF6.limit(30) // 30 updates to inserts added after insert overwrite table. if not for this, updates generated from datagne,
    // could split as inserts and updates from hudi standpoint due to insert overwrite table operation.
    inputDF9.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.clean.automatic", "true")
      .option("hoodie.keep.min.commits", "16")
      .option("hoodie.keep.max.commits", "17")
      .option("hoodie.clean.commits.retained", "15")
      .option("hoodie.compact.inline", "false")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant9 = metaClient.reloadActiveTimeline.lastInstant().get()
    // in cases that there is log files, it will NOT write out cdc log files.
    // But instant9 is the clean instant, not the upsert one. so we omit to test.
    val commitTime9 = instant9.requestedTime
    val cntForInstant9 = spark.read.format("hudi").load(basePath).count()
    val cdcDataOnly9 = cdcDataFrame(commitTime8)
    val insertedCnt9 = cntForInstant9 - cntForInstant6 - 10
    val updatedCnt9 = 30 - insertedCnt9
    assertCDCOpCnt(cdcDataOnly9, insertedCnt9, updatedCnt9, 0)

    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt + 30, totalDeletedCnt)
  }

  /**
   * Step1: Insert Data 100
   * Step2: Insert Overwrite Partition
   * Step3: Delete Partition
   * Step4: Upsert
   */
  @ParameterizedTest
  @CsvSource(Array(
    "COPY_ON_WRITE,data_before_after", "MERGE_ON_READ,data_before_after",
    "COPY_ON_WRITE,data_before", "MERGE_ON_READ,data_before",
    "COPY_ON_WRITE,op_key_only", "MERGE_ON_READ,op_key_only"))
  def testDataSourceWriteWithPartitionField(tableType: String, loggingMode: String): Unit = {
    val options = commonOpts ++ Map(
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> loggingMode
    )

    var totalInsertedCnt = 0L
    var totalUpdatedCnt = 0L
    var totalDeletedCnt = 0L
    var allVisibleCDCData = spark.emptyDataFrame

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val partitionToCnt = spark.read.format("hudi").load(basePath)
      .groupBy("partition").count().collect()
      .map(row => row.getString(0) -> row.getLong(1)).toMap
    assert(partitionToCnt.contains(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH))
    assert(partitionToCnt.contains(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH))

    // init meta client
    metaClient = createMetaClient(spark, basePath)

    totalInsertedCnt += 100
    val instant1 = metaClient.reloadActiveTimeline.lastInstant().get()
    // all the data is new-coming, it will write out cdc log files.
    assertFalse(hasCDCLogFile(instant1))
    val commitTime1 = instant1.requestedTime
    val cdcDataOnly1 = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly1, 100, 0, 0)

    // Insert Overwrite Partition Operation
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("001", 30, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant2 = metaClient.reloadActiveTimeline.lastInstant().get()
    // the files which keep all the old data will be replaced directly.
    // and all the new data will write out some new file groups.
    // it will NOT write out cdc log files
    assertFalse(hasCDCLogFile(instant2))
    val commitTime2 = instant2.requestedTime
    val insertedCnt2 = 30
    val deletedCnt2 = partitionToCnt(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)
    val cdcDataOnly2 = cdcDataFrame((commitTime2.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly2, insertedCnt2, 0, deletedCnt2)

    totalInsertedCnt += insertedCnt2
    totalDeletedCnt += deletedCnt2
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // Drop Partition
    spark.emptyDataFrame.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_PARTITION_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key(), HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant3 = metaClient.reloadActiveTimeline.lastInstant().get()
    // the files belongs to this partition will be replaced directly.
    // it will NOT write out cdc log files.
    assertFalse(hasCDCLogFile(instant3))
    val commitTime3 = instant3.requestedTime
    val cntForInstant3 = spark.read.format("hudi").load(basePath).count()
    // here we use `commitTime2` to query the change data in commit 3.
    // because `commitTime3` is the ts of the clustering operation, not the delete operation.
    val cdcDataOnly3 = cdcDataFrame((commitTime3.toLong - 1).toString)
    val deletedCnt3 = partitionToCnt(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH)
    assertCDCOpCnt(cdcDataOnly3, 0, 0, deletedCnt3)

    totalDeletedCnt += deletedCnt3
    // all the change data  in the range [commitTime1, commitTime3]
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // Upsert Operation
    val records4 = recordsToStrings(dataGen.generateUniqueUpdates("000", 50)).asScala.toList
    val inputDF4 = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant4 = metaClient.reloadActiveTimeline.lastInstant().get()
    val commitTime4 = instant4.requestedTime
    val cntForInstant4 = spark.read.format("hudi").load(basePath).count()
    val cdcDataOnly4 = cdcDataFrame((commitTime4.toLong - 1).toString)
    val insertedCnt4 = cntForInstant4 - cntForInstant3
    val updatedCnt4 = 50 - insertedCnt4
    assertCDCOpCnt(cdcDataOnly4, insertedCnt4, updatedCnt4, 0)

    totalInsertedCnt += insertedCnt4
    totalUpdatedCnt += updatedCnt4
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // check both starting and ending are provided
    val cdcDataFrom2To3 = cdcDataFrame((commitTime2.toLong - 1).toString, commitTime3)
    assertCDCOpCnt(cdcDataFrom2To3, insertedCnt2, 0, deletedCnt2 + deletedCnt3)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieCDCSupplementalLoggingMode])
  def testCDCWithMultiBlocksAndLogFiles(loggingMode: HoodieCDCSupplementalLoggingMode): Unit = {
    val (blockSize, logFileSize) = if (loggingMode == OP_KEY_ONLY) {
      // only op and key will be stored in cdc log file, we set the smaller values for the two configs.
      // so that it can also write out more than one cdc log file
      // and each of cdc log file has more that one data block as we expect.
      (256, 1024)
    } else {
      (2048, 5120)
    }
    val options = commonOpts ++ Map(
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> loggingMode.name(),
      "hoodie.logfile.data.block.max.size" -> blockSize.toString,
      "hoodie.logfile.max.size" -> logFileSize.toString
    )

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = createMetaClient(spark, basePath)

    val schemaResolver = new TableSchemaResolver(metaClient)
    val dataSchema = schemaResolver.getTableAvroSchema(false)
    val cdcSchema = schemaBySupplementalLoggingMode(loggingMode, dataSchema)

    // Upsert Operation
    val hoodieRecords2 = dataGen.generateUniqueUpdates("001", 50)
    val records2 = recordsToStrings(hoodieRecords2).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant2 = metaClient.reloadActiveTimeline.lastInstant().get()

    // check cdc data
    val cdcLogFiles2 = getCDCLogFile(instant2)
    val cdcDataFromCDCLogFile2 = cdcLogFiles2.flatMap(readCDCLogFile(_, cdcSchema))
    // check the num of cdc data
    assertEquals(cdcDataFromCDCLogFile2.size, 50)
    // check record key, before, after according to the supplemental logging mode
    checkCDCDataForInsertOrUpdate(loggingMode, cdcSchema, dataSchema,
      cdcDataFromCDCLogFile2, hoodieRecords2, HoodieCDCOperation.UPDATE)

    val commitTime2 = instant2.requestedTime
    var currentSnapshotData = spark.read.format("hudi").load(basePath)
    // at the last commit, 100 records are inserted.
    val insertedCnt2 = currentSnapshotData.count() - 100
    val updatedCnt2 = 50 - insertedCnt2
    val cdcDataOnly2 = cdcDataFrame((commitTime2.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly2, insertedCnt2, updatedCnt2, 0)
  }

  @Test
  def testCDCWithAWSDMSPayload(): Unit = {
    val options = Map(
      "hoodie.table.name" -> "test",
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.precombine.field" -> "replicadmstimestamp",
      "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.keygen.NonpartitionedKeyGenerator",
      "hoodie.datasource.write.partitionpath.field" -> "",
      "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.AWSDmsAvroPayload",
      DataSourceWriteOptions.RECORD_MERGE_MODE.key() -> RecordMergeMode.CUSTOM.name(),
      "hoodie.table.cdc.enabled" -> "true",
      "hoodie.table.cdc.supplemental.logging.mode" -> "data_before_after"
    )

    val data: Seq[(String, String, String, String)] = Seq(
      ("1", "I", "2023-06-14 15:46:06.953746", "A"),
      ("2", "I", "2023-06-14 15:46:07.953746", "B"),
      ("3", "I", "2023-06-14 15:46:08.953746", "C")
    )

    val schema: StructType = StructType(Seq(
      StructField("id", StringType),
      StructField("Op", StringType),
      StructField("replicadmstimestamp", StringType),
      StructField("code", StringType)
    ))

    val df = spark.createDataFrame(data.map(Row.fromTuple).asJava, schema)
    df.write
      .format("org.apache.hudi")
      .option("hoodie.datasource.write.operation", "upsert")
      .options(options)
      .mode("append")
      .save(basePath)

    assertEquals(spark.read.format("org.apache.hudi").load(basePath).count(), 3)

    val newData: Seq[(String, String, String, String)] = Seq(
      ("3", "D", "2023-06-14 15:47:09.953746", "B")
    )

    val newDf = spark.createDataFrame(newData.map(Row.fromTuple).asJava, schema)

    newDf.write
      .format("org.apache.hudi")
      .option("hoodie.datasource.write.operation", "upsert")
      .options(options)
      .mode("append")
      .save(basePath)

    assertEquals(spark.read.format("org.apache.hudi").load(basePath).count(), 2)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieCDCSupplementalLoggingMode])
  def testCDCCleanRetain(loggingMode: HoodieCDCSupplementalLoggingMode): Unit = {
    val options = Map(
      "hoodie.table.cdc.enabled" -> "true",
      "hoodie.table.cdc.supplemental.logging.mode" -> loggingMode.name(),
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      "hoodie.datasource.write.recordkey.field" -> "_row_key",
      "hoodie.datasource.write.precombine.field" -> "timestamp",
      "hoodie.table.name" -> ("hoodie_test" + loggingMode.name()),
      "hoodie.clean.automatic" -> "true",
      "hoodie.clean.commits.retained" -> "1"
    )

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = createMetaClient(spark, basePath)

    // Upsert Operation
    val hoodieRecords2 = dataGen.generateUniqueUpdates("001", 50)
    val records2 = recordsToStrings(hoodieRecords2).asScala.toList
    val inputDF2 = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.datasource.write.operation", "upsert")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant2 = metaClient.reloadActiveTimeline.lastInstant().get()
    val cdcLogFiles2 = getCDCLogFile(instant2)
    assertTrue(isFilesExistInFileSystem(cdcLogFiles2))

    // Upsert Operation
    val hoodieRecords3 = dataGen.generateUniqueUpdates("002", 50)
    val records3 = recordsToStrings(hoodieRecords3).asScala.toList
    val inputDF3 = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.datasource.write.operation", "upsert")
      .mode(SaveMode.Append)
      .save(basePath)

    // Upsert Operation
    val hoodieRecords4 = dataGen.generateUniqueUpdates("003", 50)
    val records4 = recordsToStrings(hoodieRecords4).asScala.toList
    val inputDF4 = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.datasource.write.operation", "upsert")
      .mode(SaveMode.Append)
      .save(basePath)
    assertFalse(isFilesExistInFileSystem(cdcLogFiles2))
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieCDCSupplementalLoggingMode])
  def testCDCWhenFirstWriteContainsUpsertAndDelete(loggingMode: HoodieCDCSupplementalLoggingMode): Unit = {
      val schema = StructType(List(
        StructField("_id", StringType, nullable = true),
        StructField("Op", StringType, nullable = true),
        StructField("replicadmstimestamp", StringType, nullable = true),
        StructField("code", StringType, nullable = true),
        StructField("partition", StringType, nullable = true)
      ))

      val rdd1 = spark.sparkContext.parallelize(Seq(
        Row("1", "I", "2023-06-14 15:46:06.953746", "A", "A"),
        Row("1", "U", "2023-06-20 15:46:06.953746", "A", "A"),
        Row("2", "I", "2023-06-14 15:46:06.953746", "A", "A"),
        Row("2", "D", "2023-06-20 15:46:06.953746", "A", "A")
      ))
      val df1 = spark.createDataFrame(rdd1, schema)
      df1.write.format("hudi")
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), MOR_TABLE_TYPE_OPT_VAL)
        .options(getQuickstartWriteConfigs)
        .option(RECORDKEY_FIELD_OPT_KEY, "_id")
        .option(PRECOMBINE_FIELD_OPT_KEY, "replicadmstimestamp")
        .option(PARTITIONPATH_FIELD_OPT_KEY, "partition")
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName + loggingMode.name())
        .option("hoodie.datasource.write.operation", "upsert")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
        .option("hoodie.datasource.write.payload.class", "org.apache.hudi.common.model.AWSDmsAvroPayload")
        .option(DataSourceWriteOptions.RECORD_MERGE_MODE.key(), RecordMergeMode.CUSTOM.name())
        .option("hoodie.table.cdc.enabled", "true")
        .option("hoodie.table.cdc.supplemental.logging.mode", loggingMode.name())
        .mode(SaveMode.Append).save(basePath)

      val rdd2 = spark.sparkContext.parallelize(Seq(
        Row("1", "U", "2023-06-14 15:46:06.953746", "A", "A"),
        Row("2", "U", "2023-06-20 15:46:06.953746", "A", "A"),
        Row("3", "I", "2023-06-20 15:46:06.953746", "A", "A")
      ))
      val df2 = spark.createDataFrame(rdd2, schema)
      df2.write.format("hudi")
        .option(DataSourceWriteOptions.TABLE_TYPE.key(), MOR_TABLE_TYPE_OPT_VAL)
        .options(getQuickstartWriteConfigs)
        .option(RECORDKEY_FIELD_OPT_KEY, "_id")
        .option(PRECOMBINE_FIELD_OPT_KEY, "replicadmstimestamp")
        .option(PARTITIONPATH_FIELD_OPT_KEY, "partition")
        .option(HoodieWriteConfig.TBL_NAME.key(), tableName + loggingMode.name())
        .option("hoodie.datasource.write.operation", "upsert")
        .option("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.ComplexKeyGenerator")
        .option("hoodie.datasource.write.payload.class", "org.apache.hudi.common.model.AWSDmsAvroPayload")
        .option(DataSourceWriteOptions.RECORD_MERGE_MODE.key(), RecordMergeMode.CUSTOM.name())
        .option("hoodie.table.cdc.enabled", "true")
        .option("hoodie.table.cdc.supplemental.logging.mode", loggingMode.name())
        .mode(SaveMode.Append).save(basePath)

    val metaClient = createMetaClient(spark, basePath)
      val startTimeStamp = metaClient.reloadActiveTimeline().firstInstant().get.requestedTime
      val latestTimeStamp = metaClient.reloadActiveTimeline().lastInstant().get.requestedTime

      val result1 = spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", "0")
        .option("hoodie.datasource.read.end.instanttime", startTimeStamp)
        .option("hoodie.datasource.query.incremental.format", "cdc")
        .load(basePath)
      result1.show(false)
      assertCDCOpCnt(result1, 1, 0, 0)
      assertEquals(result1.count(), 1)

      val result2 = spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", startTimeStamp)
        .option("hoodie.datasource.read.end.instanttime", latestTimeStamp)
        .option("hoodie.datasource.query.incremental.format", "cdc")
        .load(basePath)
      result2.show(false)
      assertCDCOpCnt(result2, 2, 1, 0)
      assertEquals(result2.count(), 3)

      val result3 = spark.read.format("hudi")
        .option("hoodie.datasource.query.type", "incremental")
        .option("hoodie.datasource.read.begin.instanttime", "0")
        .option("hoodie.datasource.read.end.instanttime", latestTimeStamp)
        .option("hoodie.datasource.query.incremental.format", "cdc")
        .load(basePath)
      result3.show(false)
      assertCDCOpCnt(result3, 3, 1, 0)
      assertEquals(result3.count(), 4)
    }
}
