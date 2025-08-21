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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieReaderConfig}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieCommitMetadata, HoodieLogFile, HoodieTableType, WriteConcurrencyMode}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.log.HoodieLogFileReader
import org.apache.hudi.common.table.view.FileSystemViewManager
import org.apache.hudi.common.testutils.{HoodieTestDataGenerator, HoodieTestUtils}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieIndexConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.HoodieIndex.IndexType.{BUCKET, SIMPLE}
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConverters._

@Tag("functional")
class TestMORDataSourceStorage extends SparkClientFunctionalTestHarness {

  override def conf: SparkConf = conf(getSparkSqlConf)

  @ParameterizedTest
  @CsvSource(Array(
    "true,,false",
    "true,fare.currency,false",
    "false,,false",
    "false,fare.currency,true"
  ))
  def testMergeOnReadStorage(isMetadataEnabled: Boolean, orderingFields: String, useFileGroupReader: Boolean): Unit = {
    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS.key -> "10"
    )
    val verificationCol: String = "driver"
    val updatedVerificationVal: String = "driver_update"

    var options: Map[String, String] = commonOpts +
      (HoodieMetadataConfig.ENABLE.key -> String.valueOf(isMetadataEnabled))
    if (!StringUtils.isNullOrEmpty(orderingFields)) {
      options += (HoodieTableConfig.ORDERING_FIELDS.key() -> orderingFields)
    }
    if (useFileGroupReader) {
      options += (HoodieReaderConfig.FILE_GROUP_READER_ENABLED.key() -> String.valueOf(useFileGroupReader))
    }
    val dataGen = new HoodieTestDataGenerator(0xDEEF)
    val fs = HadoopFSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Bulk Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Read RO View
    val hudiRODF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)

    assertEquals(100, hudiRODF1.count()) // still 100, since we only updated
    val insertCommitTime = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val insertCommitTimes = hudiRODF1.select("_hoodie_commit_time").distinct().collectAsList().asScala.map(r => r.getString(0)).toList
    assertEquals(List(insertCommitTime), insertCommitTimes)

    // Upsert operation without Hudi metadata columns
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).asScala.toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    // Read Snapshot query
    val updateCommitTime = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)

    val updateCommitTimes = hudiSnapshotDF2.select("_hoodie_commit_time").distinct().collectAsList().asScala.map(r => r.getString(0)).toList
    assertEquals(List(updateCommitTime), updateCommitTimes)

    // Upsert based on the written table with Hudi metadata columns
    val verificationRowKey = hudiSnapshotDF2.limit(1).select("_row_key").first.getString(0)
    val inputDF3 = hudiSnapshotDF2.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))

    inputDF3.write.format("org.apache.hudi")
      .options(options)
      .mode(SaveMode.Append)
      .save(basePath)

    val hudiSnapshotDF3 = spark.read.format("hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)
    assertEquals(100, hudiSnapshotDF3.count())
    assertEquals(updatedVerificationVal, hudiSnapshotDF3.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))
  }

  @Test
  def testMergeOnReadStorageDefaultCompaction(): Unit = {
    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      "hoodie.merge.small.file.group.candidates.limit" -> "0",
      HoodieWriteConfig.WRITE_RECORD_POSITIONS.key -> "true",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
    )

    var options: Map[String, String] = commonOpts
    val dataGen = new HoodieTestDataGenerator(0xDEEF)
    val fs = HadoopFSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Bulk Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(options)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    val hudiDF1 = spark.read.format("org.apache.hudi")
      .load(basePath)

    assertEquals(100, hudiDF1.count())

    // upsert
    for ( a <- 1 to 5) {
      val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).asScala.toList
      val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
      inputDF2.write.format("org.apache.hudi")
        .options(options)
        .mode(SaveMode.Append)
        .save(basePath)
    }
    // compaction should have been completed
    val metaClient = HoodieTestUtils.createMetaClient(new HadoopStorageConfiguration(fs.getConf), basePath)
    assertEquals(1, metaClient.getActiveTimeline.getCommitAndReplaceTimeline.countInstants())
    assertEquals(100, hudiDF1.count())
  }

  @ParameterizedTest
  @CsvSource(value = Array("false,false", "true,true", "true,false"))
  def testAutoDisablingRecordPositionsUnderPendingCompaction(writeRecordPosition: Boolean,
                                                             enableNBCC: Boolean): Unit = {
    val options = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      "hoodie.bulkinsert.shuffle.parallelism" -> "2",
      "hoodie.delete.shuffle.parallelism" -> "1",
      "hoodie.merge.small.file.group.candidates.limit" -> "0",
      HoodieWriteConfig.WRITE_RECORD_POSITIONS.key -> writeRecordPosition.toString,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getName,
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      HoodieIndexConfig.INDEX_TYPE.key -> (if (enableNBCC) BUCKET.name else SIMPLE.name),
      HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key() -> classOf[InProcessLockProvider].getName,
      HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> (
        if (enableNBCC) {
          WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name
        } else {
          WriteConcurrencyMode.SINGLE_WRITER.name
        }),
      HoodieTableConfig.TYPE.key -> HoodieTableType.MERGE_ON_READ.name())
    val optionWithoutCompactionExecution = options ++ Map(
      HoodieCompactionConfig.INLINE_COMPACT.key -> "false",
      HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "2")
    val optionWithCompactionExecution = options ++ Map(
      HoodieCompactionConfig.INLINE_COMPACT.key -> "true",
      HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key -> "false",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "6")

    val dataGen = new HoodieTestDataGenerator(0xDEEF)
    val storage = HoodieStorageUtils.getStorage(
      basePath, new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration))
    // Bulk Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 1))
    inputDF1.write.format("hudi")
      .options(optionWithoutCompactionExecution)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(storage, basePath, "000"))

    assertEquals(100, spark.read.format("hudi")
      .option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key, "true").load(basePath).count())

    val metaClient = HoodieTestUtils.createMetaClient(storage.getConf, basePath)
    var logFileList = List[HoodieLogFile]()
    // Upsert
    for (i <- 1 to 3) {
      // Generate some deletes so that if the record positions are still enabled during pending
      // compaction, the positions of the log files generated for the base file under pending
      // compaction can be wrong.
      val updates2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 5)).asScala.toList
      val deletes2 = recordsToStrings(dataGen.generateUniqueDeleteRecords("002", 15)).asScala.toList
      val inputDF2: Dataset[Row] =
        if (i == 3) {
          spark.read.json(spark.sparkContext.parallelize(deletes2, 2))
        } else {
          spark.read.json(spark.sparkContext.parallelize(updates2, 2))
            .union(spark.read.json(spark.sparkContext.parallelize(deletes2, 2)))
        }
      inputDF2.write.format("hudi")
        .options(optionWithoutCompactionExecution)
        .option(DataSourceWriteOptions.OPERATION.key,
          if (i == 3) {
            DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL
          } else {
            DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL
          })
        .mode(SaveMode.Append)
        .save(basePath)

      assertEquals(100 - i * 15, spark.read.format("hudi")
        .option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key, "true").load(basePath).count())
      // Compaction should be scheduled and pending
      assertEquals(1,
        metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().countInstants())
      assertEquals(i + 1, metaClient.getActiveTimeline.getDeltaCommitTimeline.countInstants())
      // The deltacommit from all three rounds should write record positions in the log files
      // and the base file instant time of the record positions should match the latest
      // base file before compaction happens
      logFileList = validateRecordPositionsInLogFiles(
        metaClient, writeRecordPosition && !enableNBCC)
    }

    for (i <- 4 to 6) {
      // Trigger compaction execution
      val updates3 = recordsToStrings(dataGen.generateUniqueUpdates("004", 5)).asScala.toList
      val deletes3 = recordsToStrings(dataGen.generateUniqueDeleteRecords("004", 15)).asScala.toList
      val inputDF3: Dataset[Row] =
        if (i == 3) {
          spark.read.json(spark.sparkContext.parallelize(deletes3, 2))
        } else {
          spark.read.json(spark.sparkContext.parallelize(updates3, 2))
            .union(spark.read.json(spark.sparkContext.parallelize(deletes3, 2)))
        }
      inputDF3.write.format("hudi")
        .options(optionWithCompactionExecution)
        .option(DataSourceWriteOptions.OPERATION.key,
          if (i == 3) {
            DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL
          } else {
            DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL
          })
        .mode(SaveMode.Append)
        .save(basePath)

      assertEquals(100 - i * 15, spark.read.format("hudi")
        .option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key, "true").load(basePath).count())
      // Compaction should complete
      assertTrue(metaClient.reloadActiveTimeline().filterPendingCompactionTimeline().empty())
      assertEquals(1, metaClient.getActiveTimeline.getCommitAndReplaceTimeline.countInstants())
      assertEquals(i + 1, metaClient.getActiveTimeline.getDeltaCommitTimeline.countInstants())
      // The deltacommit from the forth round should write record positions in the log files
      // but the base file instant time of the record positions should not match the latest
      // base file, since the compaction happens afterwards.
      if (i == 4) {
        // Also revalidate the log files generate from the third round as the base instant
        // time should not match the the latest base file after compaction
        validateRecordPositionsInLogFiles(
          metaClient, shouldContainRecordPosition = writeRecordPosition && !enableNBCC,
          logFileList, shouldBaseFileInstantTimeMatch = false)
      }
      // The deltacommit from the fifth and sixth round should write record positions in the log
      // files and the base file instant time of the record positions should match the latest
      // base file generated by the compaction.
      validateRecordPositionsInLogFiles(
        metaClient, shouldContainRecordPosition = writeRecordPosition && !enableNBCC,
        shouldBaseFileInstantTimeMatch = i != 4)
    }
  }

  def validateRecordPositionsInLogFiles(metaClient: HoodieTableMetaClient,
                                        shouldContainRecordPosition: Boolean,
                                        shouldBaseFileInstantTimeMatch: Boolean = true): List[HoodieLogFile] = {
    val instant = metaClient.getActiveTimeline.getDeltaCommitTimeline.lastInstant().get()
    val commitMetadata = metaClient.getActiveTimeline.readCommitMetadata(instant)
    val logFileList: List[HoodieLogFile] = commitMetadata.getFileIdAndFullPaths(metaClient.getBasePath)
      .asScala.values
      .filter(e => FSUtils.isLogFile(new StoragePath(e)))
      .map(e => new HoodieLogFile(new StoragePath(e)))
      .toList
    assertFalse(logFileList.isEmpty)
    validateRecordPositionsInLogFiles(
      metaClient, shouldContainRecordPosition, logFileList, shouldBaseFileInstantTimeMatch)
    logFileList
  }

  def validateRecordPositionsInLogFiles(metaClient: HoodieTableMetaClient,
                                        shouldContainRecordPosition: Boolean,
                                        logFileList: List[HoodieLogFile],
                                        shouldBaseFileInstantTimeMatch: Boolean): Unit = {
    val schema = new TableSchemaResolver(metaClient).getTableAvroSchema
    val fsv = FileSystemViewManager.createInMemoryFileSystemView(
      context(), metaClient, HoodieMetadataConfig.newBuilder().build())
    logFileList.foreach(filename => {
      val logFormatReader = new HoodieLogFileReader(metaClient.getStorage, filename, schema, 81920)
      var numBlocks = 0
      while (logFormatReader.hasNext) {
        val logBlock = logFormatReader.next()
        val recordPositions = logBlock.getRecordPositions
        assertEquals(shouldContainRecordPosition, !recordPositions.isEmpty)
        if (shouldContainRecordPosition) {
          val baseFile = fsv.getLatestBaseFile("", filename.getFileId)
          assertTrue(baseFile.isPresent)
          assertEquals(
            shouldBaseFileInstantTimeMatch,
            baseFile.get().getCommitTime.equals(logBlock.getBaseFileInstantTimeOfPositions))
        }
        numBlocks += 1
      }
      logFormatReader.close()
      assertTrue(numBlocks > 0)
    })
  }
}
