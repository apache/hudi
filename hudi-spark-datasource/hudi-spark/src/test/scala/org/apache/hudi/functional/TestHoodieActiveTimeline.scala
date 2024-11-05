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

import org.apache.hudi.common.model.HoodieFileFormat
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase
import org.apache.hudi.{DataSourceWriteOptions, HoodieDataSourceHelpers}

import org.apache.spark.sql._
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Tests on HoodieActionTimeLine using the real hudi table.
 */
class TestHoodieActiveTimeline extends HoodieSparkClientTestBase {

  var spark: SparkSession = null

  private val log = LoggerFactory.getLogger(classOf[TestHoodieActiveTimeline])

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach
  override def setUp() {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @Test
  def testGetLastCommitMetadataWithValidDataForCOW(): Unit = {
    // First Operation:
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val commit1Time = HoodieDataSourceHelpers.latestCommit(storage, basePath)
    val partitionsForCommit1 = spark.read.format("org.apache.hudi").load(basePath)
      .select("_hoodie_partition_path")
      .distinct().collect()
      .map(_.get(0).toString).sorted
    assert(Array("2015/03/16", "2015/03/17", "2016/03/15").sameElements(partitionsForCommit1))

    val metaClient: HoodieTableMetaClient = createMetaClient(basePath)
    var activeTimeline = metaClient.getActiveTimeline

    // check that get the latest parquet file
    val ret1 = activeTimeline.getLastCommitMetadataWithValidData()
    assert(ret1.isPresent)
    val (instant1, commitMetadata1) = (ret1.get().getLeft, ret1.get().getRight)
    assertEquals(instant1.getTimestamp, commit1Time)
    val relativePath1 = commitMetadata1.getFileIdAndRelativePaths.values().stream().findAny().get()
    assert(relativePath1.contains(commit1Time))
    assert(relativePath1.contains(HoodieFileFormat.PARQUET.getFileExtension))

    // Second Operation:
    // Drop Partition on 2015/03/16
    spark.emptyDataFrame.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.DELETE_PARTITION_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.PARTITIONS_TO_DELETE.key, "2015/03/16")
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2Time = HoodieDataSourceHelpers.latestCommit(storage, basePath)
    val countPartitionDropped = spark.read.format("org.apache.hudi").load(basePath)
      .where("_hoodie_partition_path = '2015/03/16'").count()
    assertEquals(countPartitionDropped, 0)

    // DropPartition will not generate a file with valid data. Get the prev instant and metadata.
    activeTimeline = activeTimeline.reload()
    val ret2 = activeTimeline.getLastCommitMetadataWithValidData()
    assert(ret2.isPresent)
    val (instant2, commitMetadata2) = (ret2.get().getLeft, ret2.get().getRight)
    assertEquals(instant2.getTimestamp, commit1Time)
    val relativePath2 = commitMetadata2.getFileIdAndRelativePaths.values().stream().findAny().get()
    assert(relativePath2.contains(commit1Time))
    assert(relativePath2.contains(HoodieFileFormat.PARQUET.getFileExtension))

    // Third Operation:
    // Upsert with 50 duplicate records. Produced the second log file for each parquet.
    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("003", 50)).asScala.toList
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit3Time = HoodieDataSourceHelpers.latestCommit(storage, basePath)

    // check that get the latest parquet file generated by compaction
    activeTimeline = activeTimeline.reload()
    val ret3 = activeTimeline.getLastCommitMetadataWithValidData()
    assert(ret3.isPresent)
    val (instant3, commitMetadata3) = (ret3.get().getLeft, ret3.get().getRight)
    assertEquals(instant3.getTimestamp, commit3Time)
    val relativePath3 = commitMetadata3.getFileIdAndRelativePaths.values().stream().findAny().get()
    assert(relativePath3.contains(commit3Time))
    assert(relativePath3.contains(HoodieFileFormat.PARQUET.getFileExtension))
  }

  @Test
  def testGetLastCommitMetadataWithValidDataForMOR(): Unit = {
    // First Operation:
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).asScala.toList
    val inputDF1 = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false")
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val commit1Time = HoodieDataSourceHelpers.latestCommit(storage, basePath)

    val metaClient: HoodieTableMetaClient = createMetaClient(basePath)
    var activeTimeline = metaClient.getActiveTimeline

    // check that get the latest parquet file
    val ret1 = activeTimeline.getLastCommitMetadataWithValidData()
    assert(ret1.isPresent)
    val (instant1, commitMetadata1) = (ret1.get().getLeft, ret1.get().getRight)
    assertEquals(instant1.getTimestamp, commit1Time)
    val relativePath1 = commitMetadata1.getFileIdAndRelativePaths.values().stream().findAny().get()
    assert(relativePath1.contains(commit1Time))
    assert(relativePath1.contains(HoodieFileFormat.PARQUET.getFileExtension))

    // Second Operation:
    // Upsert with duplicate records. Produced a log file for each parquet.
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).asScala.toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit2Time = HoodieDataSourceHelpers.latestCommit(storage, basePath)

    // check that get the latest .log file
    activeTimeline = activeTimeline.reload()
    val ret2 = activeTimeline.getLastCommitMetadataWithValidData()
    assert(ret2.isPresent)
    val (instant2, commitMetadata2) = (ret2.get().getLeft, ret2.get().getRight)
    assertEquals(instant2.getTimestamp, commit2Time)
    val relativePath2 = commitMetadata2.getFileIdAndRelativePaths.values().stream().findAny().get()
    // deltacommit: .log file should contain the timestamp from base parquet file.
    assert(relativePath2.contains(commit1Time))
    assert(relativePath2.contains(HoodieFileFormat.HOODIE_LOG.getFileExtension))

    // Third Operation:
    // Upsert with 50 duplicate records. Produced the second log file for each parquet.
    // And trigger compaction.
    val records3 = recordsToStrings(dataGen.generateUniqueUpdates("003", 50)).asScala.toList
    val inputDF3: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records3, 2))
    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts).option("hoodie.compact.inline", "true")
      .option("hoodie.compact.inline.max.delta.commits", "1")
      .mode(SaveMode.Append).save(basePath)
    val commit3Time = HoodieDataSourceHelpers.latestCommit(storage, basePath)

    // check that get the latest parquet file generated by compaction
    activeTimeline = activeTimeline.reload()
    val ret3 = activeTimeline.getLastCommitMetadataWithValidData()
    assert(ret3.isPresent)
    val (instant3, commitMetadata3) = (ret3.get().getLeft, ret3.get().getRight)
    assertEquals(instant3.getTimestamp, commit3Time)
    val relativePath3 = commitMetadata3.getFileIdAndRelativePaths.values().stream().findAny().get()
    assert(relativePath3.contains(commit3Time))
    assert(relativePath3.contains(HoodieFileFormat.PARQUET.getFileExtension))

    // Fourth Operation:
    // Upsert with 50 duplicate records.
    val records4 = recordsToStrings(dataGen.generateUniqueUpdates("004", 50)).asScala.toList
    val inputDF4: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records4, 2))
    inputDF4.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)
    val commit4Time = HoodieDataSourceHelpers.latestCommit(storage, basePath)

    activeTimeline = activeTimeline.reload()
    val ret4 = activeTimeline.getLastCommitMetadataWithValidData()
    assert(ret4.isPresent)
    val (instant4, commitMetadata4) = (ret4.get().getLeft, ret4.get().getRight)
    assertEquals(instant4.getTimestamp, commit4Time)
    val relativePath4 = commitMetadata4.getFileIdAndRelativePaths.values().stream().findAny().get()
    // deltacommit: .log file should contain the timestamp from base parquet file.
    assert(relativePath4.contains(commit3Time))
    assert(relativePath4.contains(HoodieFileFormat.HOODIE_LOG.getFileExtension))
  }
}
