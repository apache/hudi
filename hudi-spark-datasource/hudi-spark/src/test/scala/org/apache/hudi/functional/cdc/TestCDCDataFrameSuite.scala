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
import org.apache.hudi.common.table.cdc.{HoodieCDCOperation, HoodieCDCSupplementalLoggingMode, HoodieCDCUtils}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.{deleteRecordsToStrings, recordsToStrings}

import org.apache.spark.sql.SaveMode

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConversions._

class TestCDCDataFrameSuite extends HoodieCDCTestBase {

  /**
   * Step1: Insert 100
   * Step2: Upsert 50
   * Step3: Delete 20 With Clustering
   * Step4: Insert Overwrite 50
   * Step5: Upsert 30 With Clean
   * Step6: Bluk_Insert 20
   */
  @ParameterizedTest
  @CsvSource(Array("cdc_op_key", "cdc_data_before", "cdc_data_before_after"))
  def testCOWDataSourceWrite(cdcSupplementalLoggingMode: String): Unit = {
    val options = commonOpts ++ Map(
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> cdcSupplementalLoggingMode
    )

    var totalInsertedCnt = 0L
    var totalUpdatedCnt = 0L
    var totalDeletedCnt = 0L
    var allVisibleCDCData = spark.emptyDataFrame

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

    val schemaResolver = new TableSchemaResolver(metaClient)
    val dataSchema = schemaResolver.getTableAvroSchema(false)
    val cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(
      HoodieCDCSupplementalLoggingMode.parse(cdcSupplementalLoggingMode), dataSchema)

    totalInsertedCnt += 100
    val instant1 = metaClient.reloadActiveTimeline.lastInstant().get()
    // all the data is new-coming, it will write out cdc log files.
    assertFalse(hasCDCLogFile(instant1))
    val commitTime1 = instant1.getTimestamp
    val cdcDataOnly1 = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly1, 100, 0, 0)

    // Upsert Operation
    val hoodieRecords2 = dataGen.generateUniqueUpdates("001", 50)
    val records2 = recordsToStrings(hoodieRecords2).toList
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
    checkCDCDataForInsertOrUpdate(cdcSupplementalLoggingMode, cdcSchema, dataSchema,
      cdcDataFromCDCLogFile2, hoodieRecords2, HoodieCDCOperation.UPDATE)

    val commitTime2 = instant2.getTimestamp
    var currentSnapshotData = spark.read.format("hudi").load(basePath)
    // at the last commit, 100 records are inserted.
    val insertedCnt2 = currentSnapshotData.count() - 100
    val updatedCnt2 = 50 - insertedCnt2
    val cdcDataOnly2 = cdcDataFrame((commitTime2.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly2, insertedCnt2, updatedCnt2, 0)
    totalUpdatedCnt += updatedCnt2
    totalInsertedCnt += insertedCnt2

    // Delete Operation With Clustering Operation
    val records3 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).toList
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
    val commitTime3 = instant3.getTimestamp
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
    val records4 = recordsToStrings(dataGen.generateInserts("003", 50)).toList
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
    val commitTime4 = instant4.getTimestamp
    val cdcDataOnly4 = cdcDataFrame((commitTime4.toLong - 1).toString)
    val insertedCnt4 = 50
    val deletedCnt4 = currentSnapshotData.count()
    assertCDCOpCnt(cdcDataOnly4, insertedCnt4, 0, deletedCnt4)
    totalInsertedCnt += insertedCnt4
    totalDeletedCnt += deletedCnt4
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // Upsert Operation With Clean Operation
    val records5 = recordsToStrings(dataGen.generateUniqueUpdates("004", 30)).toList
    val inputDF5 = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.clean.automatic", "true")
      .option("hoodie.keep.min.commits", "2")
      .option("hoodie.keep.max.commits", "3")
      .option("hoodie.cleaner.commits.retained", "1")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant5 = metaClient.reloadActiveTimeline.lastInstant().get()
    // part of data are updated, it will write out cdc log files.
    // But instant5 is the clean instant, not the upsert one. so we omit to test.
    val commitTime5 = instant5.getTimestamp
    // here we use `commitTime4` to query the change data in commit 5.
    // because `commitTime5` is the ts of the clean operation, not the upsert operation.
    val cdcDataOnly5 = cdcDataFrame(commitTime4)
    val currentData = spark.read.format("hudi").load(basePath)
    val insertedCnt5 = currentData.count() - 50
    val updatedCnt5 = 30 - insertedCnt5
    assertCDCOpCnt(cdcDataOnly5, insertedCnt5, updatedCnt5, 0)
    // here cause we do the clean operation and just remain the commit4 and commit5, so we need to reset the total cnt.
    // 50 is the number of inserted records at commit 4.
    totalInsertedCnt = 50 + insertedCnt5
    totalUpdatedCnt = updatedCnt5
    totalDeletedCnt = 0
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // Bulk_Insert Operation With Clean Operation
    val records6 = recordsToStrings(dataGen.generateInserts("005", 20)).toList
    val inputDF6 = spark.read.json(spark.sparkContext.parallelize(records6, 2))
    inputDF6.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant6 = metaClient.reloadActiveTimeline.lastInstant().get()
    // the files which keep all the old data will be replaced directly.
    // and all the new data will write out some new file groups.
    // it will NOT write out cdc log files
    assertFalse(hasCDCLogFile(instant6))
    val commitTime6 = instant6.getTimestamp
    val cdcDataOnly6 = cdcDataFrame((commitTime6.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly6, 20, 0, 0)
    totalInsertedCnt += 20
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)
  }


  /**
   * Step1: Insert 100
   * Step2: Upsert 50
   * Step3: Delete 20 With Compaction
   * Step4: Bluk_Insert 100
   * Step5: Upsert 60 With Clustering
   * Step6: Insert Overwrite 70
   * Step7: Upsert 30 With CLean
   */
  @ParameterizedTest
  @CsvSource(Array("cdc_op_key", "cdc_data_before", "cdc_data_before_after"))
  def testMORDataSourceWrite(cdcSupplementalLoggingMode: String): Unit = {
    val options = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key() -> DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL,
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> cdcSupplementalLoggingMode
    )

    var totalInsertedCnt = 0L
    var totalUpdatedCnt = 0L
    var totalDeletedCnt = 0L
    var allVisibleCDCData = spark.emptyDataFrame

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

    totalInsertedCnt += 100
    val instant1 = metaClient.reloadActiveTimeline.lastInstant().get()
    // all the data is new-coming, it will write out cdc log files.
    assertFalse(hasCDCLogFile(instant1))
    val commitTime1 = instant1.getTimestamp
    val cdcDataOnly1 = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly1, 100, 0, 0)

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
    val cdcDataFromCDCLogFile2 = getCDCLogFile(instant2).flatMap(readCDCLogFile(_, cdcSchema))
    assertEquals(cdcDataFromCDCLogFile2.size, 50)
    // check op
    assertEquals(cdcDataFromCDCLogFile2.count(r => r.get(0).toString == "u"), 30)
    assertEquals(cdcDataFromCDCLogFile2.count(r => r.get(0).toString == "i"), 20)

    val commitTime2 = instant2.getTimestamp
    var currentSnapshotData = spark.read.format("hudi").load(basePath)
    // at the last commit, 100 records are inserted.
    val insertedCnt2 = currentSnapshotData.count() - 100
    val updatedCnt2 = 50 - insertedCnt2
    val cdcDataOnly2 = cdcDataFrame((commitTime2.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly2, insertedCnt2, updatedCnt2, 0)
    totalUpdatedCnt += updatedCnt2
    totalInsertedCnt += insertedCnt2

    // 3. Delete Operation With Compaction Operation
    val records3 = deleteRecordsToStrings(dataGen.generateUniqueDeletes(20)).toList
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
    val commitTime3 = instant3.getTimestamp
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
    val records4 = recordsToStrings(dataGen.generateInserts("003", 100)).toList
    val inputDF4 = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant4 = metaClient.reloadActiveTimeline.lastInstant().get()
    // all the new data will write out some new file groups.
    // it will NOT write out cdc log files
    assertFalse(hasCDCLogFile(instant4))
    val commitTime4 = instant4.getTimestamp
    val cntForInstant4 = spark.read.format("hudi").load(basePath).count()
    val cdcDataOnly4 = cdcDataFrame((commitTime4.toLong - 1).toString)
    val insertedCnt4 = 100
    assertCDCOpCnt(cdcDataOnly4, insertedCnt4, 0, 0)

    totalInsertedCnt += insertedCnt4
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // 5. Upsert Operation With Clustering Operation
    val records5 = recordsToStrings(dataGen.generateUniqueUpdates("004", 60)).toList
    val inputDF5 = spark.read.json(spark.sparkContext.parallelize(records5, 2))
    inputDF5.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "1")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant5 = metaClient.reloadActiveTimeline.lastInstant().get()
    // in cases that there is log files, it will NOT write out cdc log files.
    // But instant5 is the clustering instant, not the upsert one. so we omit to test.
    val commitTime5 = instant5.getTimestamp
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
    val records6 = recordsToStrings(dataGen.generateInserts("005", 70)).toList
    val inputDF6 = spark.read.json(spark.sparkContext.parallelize(records6, 2))
    inputDF6.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OVERWRITE_TABLE_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant6 = metaClient.reloadActiveTimeline.lastInstant().get()
    // the files which keep all the old data will be replaced directly.
    // and all the new data will write out some new file groups.
    // it will NOT write out cdc log files
    assertFalse(hasCDCLogFile(instant6))
    val commitTime6 = instant6.getTimestamp
    val cntForInstant6 = spark.read.format("hudi").load(basePath).count()
    val cdcDataOnly6 = cdcDataFrame((commitTime6.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly6, 70, 0, cntForInstant5)
    totalInsertedCnt += 70
    totalDeletedCnt += cntForInstant5
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)

    // 7. Upsert Operation With Clean Operation
    val records7 = recordsToStrings(dataGen.generateUniqueUpdates("006", 30)).toList
    val inputDF7 = spark.read.json(spark.sparkContext.parallelize(records7, 2))
    inputDF7.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.clean.automatic", "true")
      .option("hoodie.keep.min.commits", "2")
      .option("hoodie.keep.max.commits", "3")
      .option("hoodie.cleaner.commits.retained", "1")
      .mode(SaveMode.Append)
      .save(basePath)
    val instant7 = metaClient.reloadActiveTimeline.lastInstant().get()
    // in cases that there is log files, it will NOT write out cdc log files.
    // But instant7 is the clean instant, not the upsert one. so we omit to test.
    val commitTime7 = instant7.getTimestamp
    val cntForInstant7 = spark.read.format("hudi").load(basePath).count()
    val cdcDataOnly7 = cdcDataFrame(commitTime6)
    val insertedCnt7 = cntForInstant7 - cntForInstant6
    val updatedCnt7 = 30 - insertedCnt7
    assertCDCOpCnt(cdcDataOnly7, insertedCnt7, updatedCnt7, 0)

    // here cause we do the clean operation and just remain the commit6 and commit7, so we need to reset the total cnt.
    // 70 is the number of inserted records at commit 6.
    totalInsertedCnt = 70 + insertedCnt7
    totalUpdatedCnt = updatedCnt7
    totalDeletedCnt = 0
    allVisibleCDCData = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(allVisibleCDCData, totalInsertedCnt, totalUpdatedCnt, totalDeletedCnt)
  }

  /**
   * Step1: Insert Data 100
   * Step2: Insert Overwrite Partition
   * Step3: Delete Partition
   * Step4: Upsert
   */
  @ParameterizedTest
  @CsvSource(Array(
    "COPY_ON_WRITE,cdc_data_before_after", "MERGE_ON_READ,cdc_data_before_after",
    "COPY_ON_WRITE,cdc_data_before", "MERGE_ON_READ,cdc_data_before",
    "COPY_ON_WRITE,cdc_op_key", "MERGE_ON_READ,cdc_op_key"))
  def testDataSourceWriteWithPartitionField(tableType: String, cdcSupplementalLoggingMode: String): Unit = {
    val options = commonOpts ++ Map(
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> cdcSupplementalLoggingMode
    )

    var totalInsertedCnt = 0L
    var totalUpdatedCnt = 0L
    var totalDeletedCnt = 0L
    var allVisibleCDCData = spark.emptyDataFrame

    // Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("000", 100)).toList
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
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(spark.sessionState.newHadoopConf)
      .build()

    totalInsertedCnt += 100
    val instant1 = metaClient.reloadActiveTimeline.lastInstant().get()
    // all the data is new-coming, it will write out cdc log files.
    assertFalse(hasCDCLogFile(instant1))
    val commitTime1 = instant1.getTimestamp
    val cdcDataOnly1 = cdcDataFrame((commitTime1.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly1, 100, 0, 0)

    // Insert Overwrite Partition Operation
    val records2 = recordsToStrings(dataGen.generateInsertsForPartition("001", 30, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).toList
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
    val commitTime2 = instant2.getTimestamp
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
    val commitTime3 = instant3.getTimestamp
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
    val records4 = recordsToStrings(dataGen.generateUniqueUpdates("000", 50)).toList
    val inputDF4 = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)
    val instant4 = metaClient.reloadActiveTimeline.lastInstant().get()
    val commitTime4 = instant4.getTimestamp
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
  @CsvSource(Array("cdc_op_key", "cdc_data_before", "cdc_data_before_after"))
  def testCDCWithMultiBlocksAndLogFiles(cdcSupplementalLoggingMode: String): Unit = {
    val (blockSize, logFileSize) = if (cdcSupplementalLoggingMode == "cdc_op_key") {
      // only op and key will be stored in cdc log file, we set the smaller values for the two configs.
      // so that it can also write out more than one cdc log file
      // and each of cdc log file has more that one data block as we expect.
      (256, 1024)
    } else {
      (2048, 5120)
    }
    val options = commonOpts ++ Map(
      HoodieTableConfig.CDC_SUPPLEMENTAL_LOGGING_MODE.key -> cdcSupplementalLoggingMode,
      "hoodie.logfile.data.block.max.size" -> blockSize.toString,
      "hoodie.logfile.max.size" -> logFileSize.toString
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

    val cdcLogFiles2 = getCDCLogFile(instant2)
    // with a small value for 'hoodie.logfile.data.max.size',
    // it will write out >1 cdc log files due to rollover.
    assert(cdcLogFiles2.size > 1)
    // with a small value for 'hoodie.logfile.data.block.max.size',
    // it will write out >1 cdc data blocks in one single cdc log file.
    assert(getCDCBlocks(cdcLogFiles2.head, cdcSchema).size > 1)

    // check cdc data
    val cdcDataFromCDCLogFile2 = cdcLogFiles2.flatMap(readCDCLogFile(_, cdcSchema))
    // check the num of cdc data
    assertEquals(cdcDataFromCDCLogFile2.size, 50)
    // check record key, before, after according to the supplemental logging mode
    checkCDCDataForInsertOrUpdate(cdcSupplementalLoggingMode, cdcSchema, dataSchema,
      cdcDataFromCDCLogFile2, hoodieRecords2, HoodieCDCOperation.UPDATE)

    val commitTime2 = instant2.getTimestamp
    var currentSnapshotData = spark.read.format("hudi").load(basePath)
    // at the last commit, 100 records are inserted.
    val insertedCnt2 = currentSnapshotData.count() - 100
    val updatedCnt2 = 50 - insertedCnt2
    val cdcDataOnly2 = cdcDataFrame((commitTime2.toLong - 1).toString)
    assertCDCOpCnt(cdcDataOnly2, insertedCnt2, updatedCnt2, 0)
  }
}
