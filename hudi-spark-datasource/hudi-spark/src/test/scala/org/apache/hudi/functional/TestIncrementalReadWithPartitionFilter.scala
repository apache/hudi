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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem
import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem.Call
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import scala.collection.JavaConverters._

/**
 * Verifies that incremental reads apply partition filters when the commits in range touch several
 * partitions. Spark removes partition-only predicates from the post-scan filters and relies on the
 * file index to prune, so the incremental file index must evaluate them against the modified partitions.
 * Also verifies that resolving the file slices of an incremental read does not list the table.
 */
class TestIncrementalReadWithPartitionFilter extends HoodieSparkClientTestBase {

  var spark: SparkSession = null

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @BeforeEach
  override def setUp(): Unit = {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (spark != null) {
      RecordingLocalFileSystem.unregister(spark.sparkContext.hadoopConfiguration)
    }
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testIncrementalReadWithPartitionFilter(tableType: HoodieTableType): Unit = {
    writeOneCommitPerPartition(tableType)
    val incrementalDF = readIncrementally()

    assertEquals(50, incrementalDF.count())
    assertEquals(30, incrementalDF.where(s"partition = '$DEFAULT_FIRST_PARTITION_PATH'").count())
    assertEquals(20, incrementalDF.where(s"partition = '$DEFAULT_SECOND_PARTITION_PATH'").count())
    assertEquals(0, incrementalDF.where("partition = 'nonexistent'").count())
  }

  @ParameterizedTest
  @EnumSource(value = classOf[HoodieTableType])
  def testIncrementalReadDoesNotListTable(tableType: HoodieTableType): Unit = {
    writeOneCommitPerPartition(tableType)

    // Record the file system calls of the read. With the metadata table disabled, any partition
    // listing shows up as a listStatus call on storage outside the .hoodie folder.
    RecordingLocalFileSystem.register(spark.sparkContext.hadoopConfiguration)
    RecordingLocalFileSystem.reset()
    assertEquals(50, readIncrementally(HoodieMetadataConfig.ENABLE.key -> "false").count())

    // The data files are opened through the recorder, so it is known to see the calls under test.
    assertTrue(RecordingLocalFileSystem.count(Call.operation("open").and(Call.pathEndsWith(".parquet"))) > 0)
    val listings = Call.operation("listStatus", "listLocatedStatus").and(Call.underMetaFolder().negate())
    assertEquals(0, RecordingLocalFileSystem.count(listings), RecordingLocalFileSystem.describe(listings))
  }

  // One commit per partition: 30 records in the first partition, 20 in the second.
  private def writeOneCommitPerPartition(tableType: HoodieTableType): Unit = {
    Seq(("001", DEFAULT_FIRST_PARTITION_PATH, 30), ("002", DEFAULT_SECOND_PARTITION_PATH, 20)).foreach {
      case (instantTime, partition, numRecords) =>
        val records = recordsToStrings(dataGen.generateInsertsForPartition(instantTime, numRecords, partition)).asScala.toList
        spark.read.json(spark.sparkContext.parallelize(records, 2)).write.format("hudi")
          .options(commonOpts)
          .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType.name())
          .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
          .mode(SaveMode.Append)
          .save(basePath)
    }
  }

  // Reads from before the first commit, so every commit of the table is in range.
  private def readIncrementally(extraOpts: (String, String)*): DataFrame = {
    spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "000")
      // Keep the incremental listing path under test rather than the full table scan fallback.
      .option(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN.key, "false")
      .options(extraOpts.toMap)
      .load(basePath)
  }
}
