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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, DefaultSource, HoodieBaseRelation, HoodieUnsafeRDD, SparkAdapterSupport}
import org.apache.hudi.HoodieBaseRelation.projectSchema
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig, RecordMergeMode}
import org.apache.hudi.common.model.{HoodieRecord, OverwriteNonDefaultsWithLatestAvroPayload}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.{HadoopMapRedUtils, HoodieTestDataGenerator}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf

import org.apache.parquet.hadoop.util.counters.BenchmarkCounter
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.BaseRelation
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import scala.collection.JavaConverters._
import scala.math.abs

@Tag("functional")
class TestParquetColumnProjection extends SparkClientFunctionalTestHarness with Logging with SparkAdapterSupport {

  val defaultWriteOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.ENABLE.key -> "true"
    // NOTE: It's critical that we use non-partitioned table, since the way we track amount of bytes read
    //       is not robust, and works most reliably only when we read just a single file. As such, making table
    //       non-partitioned makes it much more likely just a single file will be written
  )

  override def conf: SparkConf = conf(getSparkSqlConf)

  @Test
  def testBaseFileOnlyViewRelation(): Unit = {
    val tablePath = s"$basePath/cow"
    val targetRecordsCount = 100
    val (_, schema) = bootstrapTable(tablePath, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL, targetRecordsCount,
      defaultWriteOpts, populateMetaFields = true)
    val tableState = TableState(tablePath, schema, targetRecordsCount, 0.0)

    // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
    // increases along w/ the # of columns)
    val projectedColumnsReadStats: Array[(String, Long)] =
      Array(("rider", 2363), ("rider,driver", 2463), ("rider,driver,tip_history", 3428))

    // Test COW / Snapshot
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, "", projectedColumnsReadStats)
  }

  @Test
  def testMergeOnReadSnapshotRelationWithDeltaLogs(): Unit = {
    val tablePath = s"$basePath/mor-with-logs"
    val targetRecordsCount = 100
    val targetUpdatedRecordsRatio = 0.5

    val (_, schema) = bootstrapMORTable(tablePath, targetRecordsCount, targetUpdatedRecordsRatio, defaultWriteOpts, populateMetaFields = true)
    val tableState = TableState(tablePath, schema, targetRecordsCount, targetUpdatedRecordsRatio)

    // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
    // increases along w/ the # of columns)
    val projectedColumnsReadStats: Array[(String, Long)] =
      Array(("rider", 2452), ("rider,driver", 2552), ("rider,driver,tip_history", 3517))
    // Test MOR / Snapshot / Skip-merge
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL, projectedColumnsReadStats)

    // Test MOR / Snapshot / Payload-combine
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL, projectedColumnsReadStats)

    // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
    // increases along w/ the # of columns) in Read Optimized mode (which is essentially equivalent to COW)
    val projectedColumnsReadStatsReadOptimized: Array[(String, Long)] =
      Array(("rider", 2363), ("rider,driver", 2463), ("rider,driver,tip_history", 3428))

    // Test MOR / Read Optimized
    // TODO(HUDI-3896) re-enable
    //runTest(tableState, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, "null", projectedColumnsReadStatsReadOptimized)
  }

  @Test
  def testMergeOnReadSnapshotRelationWithNoDeltaLogs(): Unit = {
    val tablePath = s"$basePath/mor-no-logs"
    val targetRecordsCount = 100
    val targetUpdatedRecordsRatio = 0.0

    val (_, schema) = bootstrapMORTable(tablePath, targetRecordsCount, targetUpdatedRecordsRatio, defaultWriteOpts, populateMetaFields = true)
    val tableState = TableState(tablePath, schema, targetRecordsCount, targetUpdatedRecordsRatio)

    //
    // Test #1: MOR table w/ Delta Logs
    //

    // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
    // increases along w/ the # of columns)
    val projectedColumnsReadStats: Array[(String, Long)] =
      Array(("rider", 2452), ("rider,driver", 2552), ("rider,driver,tip_history", 3517))

    // Test MOR / Snapshot / Skip-merge
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL, projectedColumnsReadStats)

    // Test MOR / Snapshot / Payload-combine
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL, projectedColumnsReadStats)

    // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
    // increases along w/ the # of columns) in Read Optimized mode (which is essentially equivalent to COW)
    val projectedColumnsReadStatsReadOptimized: Array[(String, Long)] =
      Array(("rider", 2363), ("rider,driver", 2463), ("rider,driver,tip_history", 3428))

    // Test MOR / Read Optimized
    // TODO(HUDI-3896) re-enable
    //runTest(tableState, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, "null", projectedColumnsReadStatsReadOptimized)
  }

  @Test
  def testMergeOnReadSnapshotRelationWithDeltaLogsFallback(): Unit = {
    val tablePath = s"$basePath/mor-with-logs-fallback"
    val targetRecordsCount = 100
    val targetUpdatedRecordsRatio = 0.5

    // NOTE: This test validates MOR Snapshot Relation falling back to read "whole" row from MOR table (as
    //       opposed to only required columns) in following cases
    //          - Non-standard Record Payload is used: such Payload might rely on the fields that are not
    //          being queried by the Spark, and we currently have no way figuring out what these fields are, therefore
    //          we fallback to read whole row
    val overriddenOpts = defaultWriteOpts ++ Map(
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key -> classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName
    )

    val (_, schema) = bootstrapMORTable(tablePath, targetRecordsCount, targetUpdatedRecordsRatio, overriddenOpts, populateMetaFields = true)
    val tableState = TableState(tablePath, schema, targetRecordsCount, targetUpdatedRecordsRatio)

    // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
    // increases along w/ the # of columns)
    val projectedColumnsReadStats: Array[(String, Long)] =
      Array(("rider", 2452), ("rider,driver", 2552), ("rider,driver,tip_history", 3517))

    // Stats for the reads fetching _all_ columns (note, how amount of bytes read
    // is invariant of the # of columns)
    val fullColumnsReadStats: Array[(String, Long)] =
      Array(("rider", 14167), ("rider,driver", 14167), ("rider,driver,tip_history", 14167))

    // Test MOR / Snapshot / Skip-merge
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL, projectedColumnsReadStats)

    // Test MOR / Snapshot / Payload-combine (using non-standard Record Payload)
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL, fullColumnsReadStats)
  }

  @Test
  def testMergeOnReadIncrementalRelationWithNoDeltaLogs(): Unit = {
    val tablePath = s"$basePath/mor-no-logs"
    val targetRecordsCount = 100
    val targetUpdatedRecordsRatio = 0.0

    val opts: Map[String, String] =
      // NOTE: Parquet Compression is disabled as it was leading to non-deterministic outcomes when testing
      //       against Spark 2.x
      defaultWriteOpts ++ Seq(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key -> "")

    val (_, schema) = bootstrapMORTable(tablePath, targetRecordsCount, targetUpdatedRecordsRatio, opts, populateMetaFields = true)
    val tableState = TableState(tablePath, schema, targetRecordsCount, targetUpdatedRecordsRatio)

    // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
    // increases along w/ the # of columns)
    val projectedColumnsReadStats: Array[(String, Long)] =
      Array(("rider", 4219), ("rider,driver", 4279), ("rider,driver,tip_history", 5186))

    val incrementalOpts: Map[String, String] = Map(
      DataSourceReadOptions.START_COMMIT.key -> "001"
    )

    // Test MOR / Incremental / Skip-merge
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL,
      projectedColumnsReadStats, incrementalOpts)

    // Test MOR / Incremental / Payload-combine
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL,
      projectedColumnsReadStats, incrementalOpts)
  }

  @Test
  def testMergeOnReadIncrementalRelationWithDeltaLogs(): Unit = {
    val tablePath = s"$basePath/mor-with-logs-incr"
    val targetRecordsCount = 100

    bootstrapMORTableWithDeltaLog(tablePath, targetRecordsCount, defaultWriteOpts, populateMetaFields = true)
    /**
     * State of timeline and updated data
     * +--------------+--------------+--------------+--------------+--------------------+--------------+--------------+--------------+
     * | timeline     | deltacommit1 | deltacommit2 | deltacommit3 | compaction.request | deltacommit4 | deltacommit5 | deltacommit6 |
     * +--------------+--------------+--------------+--------------+--------------------+--------------+--------------+--------------+
     * | updated data |      001     |      002     |      003     |                    |      004     |      005     |      006     |
     * +--------------+--------------+--------------+--------------+--------------------+--------------+--------------+--------------+
     */
    val hoodieMetaClient = createMetaClient(spark, tablePath)
    val completedCommits = hoodieMetaClient.getCommitsAndCompactionTimeline.filterCompletedInstants()
    val startUnarchivedCommitTs = completedCommits.nthInstant(1).get().requestedTime //deltacommit2
    val endUnarchivedCommitTs = completedCommits.nthInstant(5).get().requestedTime //deltacommit6

    val readOpts = defaultWriteOpts ++ Map(
      "path" -> tablePath,
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      DataSourceReadOptions.START_COMMIT.key -> startUnarchivedCommitTs,
      DataSourceReadOptions.END_COMMIT.key -> endUnarchivedCommitTs
    )

    val inputDf = spark.read.format("hudi")
      .options(readOpts)
      .load()
    val commitNum = inputDf.select("rider").distinct().collect().length
    assertTrue(commitNum > 1)
  }

  @Test
  def testMergeOnReadIncrementalRelationWithFilter(): Unit = {
    val tablePath = s"$basePath/mor-with-logs-incr-filter"
    val targetRecordsCount = 100

    bootstrapMORTableWithDeltaLog(tablePath, targetRecordsCount, defaultWriteOpts, populateMetaFields = true, inlineCompact = true)

    val hoodieMetaClient = createMetaClient(spark, tablePath)
    val completedCommits = hoodieMetaClient.getCommitsAndCompactionTimeline.filterCompletedInstants()
    val startUnarchivedCommitTs = (completedCommits.nthInstant(1).get().requestedTime.toLong - 1L).toString
    val endUnarchivedCommitTs = completedCommits.nthInstant(3).get().requestedTime //commit

    val readOpts = defaultWriteOpts ++ Map(
      "path" -> tablePath,
      DataSourceReadOptions.QUERY_TYPE.key -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      DataSourceReadOptions.START_COMMIT.key -> startUnarchivedCommitTs,
      DataSourceReadOptions.END_COMMIT.key -> endUnarchivedCommitTs
    )

    val inputDf = spark.read.format("hudi")
      .options(readOpts)
      .load()
    // Make sure the filter is not applied at the row group level
    spark.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "false")
    try {
      val rows = inputDf.select("_hoodie_commit_time").distinct().sort("_hoodie_commit_time").collect()
      assertTrue(rows.length == 2)
      assertFalse(rows.exists(_.getString(0) < startUnarchivedCommitTs))
      assertFalse(rows.exists(_.getString(0) > endUnarchivedCommitTs))
    } finally {
      spark.sessionState.conf.setConfString("spark.sql.parquet.filterPushdown", "true")
    }
  }

  // Test routine
  private def runTest(tableState: TableState,
                      queryType: String,
                      mergeType: String,
                      expectedStats: Array[(String, Long)],
                      additionalOpts: Map[String, String] = Map.empty): Unit = {
    val tablePath = tableState.path
    val readOpts = defaultWriteOpts ++ Map(
      "path" -> tablePath,
      DataSourceReadOptions.QUERY_TYPE.key -> queryType,
      DataSourceReadOptions.REALTIME_MERGE.key -> mergeType
    ) ++ additionalOpts

    val relation: BaseRelation = new DefaultSource().createRelation(spark.sqlContext, readOpts)

    relation match {
      case hoodieRelation: HoodieBaseRelation =>
        for ((columnListStr, expectedBytesRead) <- expectedStats) {
          val targetColumns = columnListStr.split(",")

          println(s"Running test for $tablePath / $queryType / $mergeType / $columnListStr")

          val (rows, bytesRead) = measureBytesRead { () =>
            val rdd = hoodieRelation.buildScan(targetColumns, Array.empty).asInstanceOf[HoodieUnsafeRDD]
            sparkAdapter.getUnsafeUtils.collect(rdd)
          }

          val targetRecordCount = tableState.targetRecordCount;
          val targetUpdatedRecordsRatio = tableState.targetUpdatedRecordsRatio

          val expectedRecordCount =
            if (DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL.equals(mergeType)) targetRecordCount * (1 + targetUpdatedRecordsRatio)
            else targetRecordCount

          assertEquals(expectedRecordCount, rows.length)
          // verify within 10% of margin.
          assertTrue((abs(expectedBytesRead - bytesRead) / expectedBytesRead) < 0.1)

          val readColumns = targetColumns ++ hoodieRelation.mandatoryFields
          val (_, projectedStructType, _) = projectSchema(Left(tableState.schema), readColumns)

          val row: InternalRow = rows.take(1).head

          // This check is mostly about making sure InternalRow deserializes properly into projected schema
          val deserializedColumns = row.toSeq(projectedStructType)
          assertEquals(readColumns.length, deserializedColumns.size)
        }
      // TODO(HUDI-7075): fix validation of parquet column projection on HadoopFsRelation
      case _ =>
    }
  }

  private def bootstrapTable(path: String,
                             tableType: String,
                             recordCount: Int,
                             opts: Map[String, String],
                             populateMetaFields: Boolean,
                             dataGenOpt: Option[HoodieTestDataGenerator] = None): (List[HoodieRecord[_]], HoodieSchema) = {
    val dataGen = dataGenOpt.getOrElse(new HoodieTestDataGenerator(0x12345))

    // Bulk Insert Operation
    val schema =
      if (populateMetaFields) HoodieTestDataGenerator.HOODIE_SCHEMA_WITH_METADATA_FIELDS
      else HoodieTestDataGenerator.HOODIE_SCHEMA

    val records = dataGen.generateInserts("001", recordCount)
    val inputDF: Dataset[Row] = toDataset(records, HoodieTestDataGenerator.HOODIE_SCHEMA)

    inputDF.write.format("org.apache.hudi")
      .options(opts)
      .option(HoodieTableConfig.POPULATE_META_FIELDS.key, populateMetaFields.toString)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(path)

    (records.asScala.toList, schema)
  }

  private def bootstrapMORTable(path: String,
                                recordCount: Int,
                                updatedRecordsRatio: Double,
                                opts: Map[String, String],
                                populateMetaFields: Boolean,
                                dataGenOpt: Option[HoodieTestDataGenerator] = None): (List[HoodieRecord[_]], HoodieSchema) = {
    val dataGen = dataGenOpt.getOrElse(new HoodieTestDataGenerator(0x12345))

    // Step 1: Bootstrap table w/ N records (t/h bulk-insert)
    val (insertedRecords, schema) = bootstrapTable(path, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL, recordCount, opts, populateMetaFields, Some(dataGen))

    if (updatedRecordsRatio == 0) {
      (insertedRecords, schema)
    } else {
      val updatesCount = (insertedRecords.length * updatedRecordsRatio).toInt
      val recordsToUpdate = insertedRecords.take(updatesCount)
      val updatedRecords = dataGen.generateUpdates("002", recordsToUpdate.asJava)

      // Step 2: Update M records out of those (t/h update)
      val inputDF = toDataset(updatedRecords, HoodieTestDataGenerator.HOODIE_SCHEMA)

      inputDF.write.format("org.apache.hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .option(HoodieTableConfig.POPULATE_META_FIELDS.key, populateMetaFields.toString)
        .mode(SaveMode.Append)
        .save(path)

      (updatedRecords.asScala.toList ++ insertedRecords.drop(updatesCount), schema)
    }
  }

  private def bootstrapMORTableWithDeltaLog(path: String,
                                recordCount: Int,
                                opts: Map[String, String],
                                populateMetaFields: Boolean,
                                dataGenOpt: Option[HoodieTestDataGenerator] = None,
                                inlineCompact: Boolean = false): (List[HoodieRecord[_]], HoodieSchema) = {
    val dataGen = dataGenOpt.getOrElse(new HoodieTestDataGenerator(0x12345))

    // Step 1: Bootstrap table w/ N records (t/h bulk-insert)
    val (insertedRecords, schema) = bootstrapTable(path, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL, recordCount, opts, populateMetaFields, Some(dataGen))

    for (i <- 2 to 6) {
      val updatesCount = (insertedRecords.length * 0.5).toInt
      val recordsToUpdate = scala.util.Random.shuffle(insertedRecords).take(updatesCount)
      val updatedRecords = dataGen.generateUpdates("%03d".format(i), recordsToUpdate.asJava)

      // Step 2: Update M records out of those (t/h update)
      val inputDF = toDataset(updatedRecords, HoodieTestDataGenerator.HOODIE_SCHEMA)

      val compactScheduleInline = if (inlineCompact) "false" else "true"
      val compactInline = if (inlineCompact) "true" else "false"

      inputDF.write.format("org.apache.hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .option(HoodieCompactionConfig.SCHEDULE_INLINE_COMPACT.key, compactScheduleInline)
        .option(HoodieCompactionConfig.INLINE_COMPACT.key, compactInline)
        .option(DataSourceWriteOptions.ASYNC_COMPACT_ENABLE.key, "false")
        .option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key, "3")
        .option(HoodieTableConfig.POPULATE_META_FIELDS.key, populateMetaFields.toString)
        .mode(SaveMode.Append)
        .save(path)
    }

    (insertedRecords, schema)
  }

  def measureBytesRead[T](f: () => T): (T, Int) = {
    // Init BenchmarkCounter to report number of bytes actually read from the Block
    BenchmarkCounter.initCounterFromReporter(HadoopMapRedUtils.createTestReporter, fs.getConf)
    val r = f.apply()
    val bytesRead = BenchmarkCounter.getBytesRead.toInt
    (r, bytesRead)
  }

  case class TableState(path: String, schema: HoodieSchema, targetRecordCount: Long, targetUpdatedRecordsRatio: Double)
}
