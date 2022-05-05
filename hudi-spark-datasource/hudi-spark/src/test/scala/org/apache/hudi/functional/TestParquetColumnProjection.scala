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

import org.apache.avro.Schema
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieRecord, OverwriteNonDefaultsWithLatestAvroPayload}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.{HadoopMapRedUtils, HoodieTestDataGenerator}
import org.apache.hudi.config.{HoodieStorageConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, DefaultSource, HoodieBaseRelation, HoodieSparkUtils, HoodieUnsafeRDD}
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{Dataset, HoodieUnsafeRDDUtils, Row, SaveMode}
import org.junit.jupiter.api.Assertions.{assertEquals, fail}
import org.junit.jupiter.api.{Disabled, Tag, Test}

import scala.collection.JavaConverters._

@Tag("functional")
class TestParquetColumnProjection extends SparkClientFunctionalTestHarness with Logging {

  val defaultWriteOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    HoodieMetadataConfig.ENABLE.key -> "true",
    // NOTE: It's critical that we use non-partitioned table, since the way we track amount of bytes read
    //       is not robust, and works most reliably only when we read just a single file. As such, making table
    //       non-partitioned makes it much more likely just a single file will be written
    DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getName
  )

  @Disabled("HUDI-3896")
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
      if (HoodieSparkUtils.isSpark3)
        Array(
          ("rider", 2363),
          ("rider,driver", 2463),
          ("rider,driver,tip_history", 3428))
      else if (HoodieSparkUtils.isSpark2)
        Array(
          ("rider", 2474),
          ("rider,driver", 2614),
          ("rider,driver,tip_history", 3629))
      else
        fail("Only Spark 3 and Spark 2 are currently supported")

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
      if (HoodieSparkUtils.isSpark3)
        Array(
          ("rider", 2452),
          ("rider,driver", 2552),
          ("rider,driver,tip_history", 3517))
      else if (HoodieSparkUtils.isSpark2)
        Array(
          ("rider", 2595),
          ("rider,driver", 2735),
          ("rider,driver,tip_history", 3750))
      else
        fail("Only Spark 3 and Spark 2 are currently supported")

    // Test MOR / Snapshot / Skip-merge
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL, projectedColumnsReadStats)

    // Test MOR / Snapshot / Payload-combine
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL, projectedColumnsReadStats)

    // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
    // increases along w/ the # of columns) in Read Optimized mode (which is essentially equivalent to COW)
    val projectedColumnsReadStatsReadOptimized: Array[(String, Long)] =
      if (HoodieSparkUtils.isSpark3)
        Array(
          ("rider", 2363),
          ("rider,driver", 2463),
          ("rider,driver,tip_history", 3428))
      else if (HoodieSparkUtils.isSpark2)
        Array(
          ("rider", 2474),
          ("rider,driver", 2614),
          ("rider,driver,tip_history", 3629))
      else
        fail("Only Spark 3 and Spark 2 are currently supported")

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
      if (HoodieSparkUtils.isSpark3)
        Array(
          ("rider", 2452),
          ("rider,driver", 2552),
          ("rider,driver,tip_history", 3517))
      else if (HoodieSparkUtils.isSpark2)
        Array(
          ("rider", 2595),
          ("rider,driver", 2735),
          ("rider,driver,tip_history", 3750))
      else
        fail("Only Spark 3 and Spark 2 are currently supported")

    // Test MOR / Snapshot / Skip-merge
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL, projectedColumnsReadStats)

    // Test MOR / Snapshot / Payload-combine
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL, projectedColumnsReadStats)

    // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
    // increases along w/ the # of columns) in Read Optimized mode (which is essentially equivalent to COW)
    val projectedColumnsReadStatsReadOptimized: Array[(String, Long)] =
      if (HoodieSparkUtils.isSpark3)
        Array(
          ("rider", 2363),
          ("rider,driver", 2463),
          ("rider,driver,tip_history", 3428))
      else if (HoodieSparkUtils.isSpark2)
        Array(
          ("rider", 2474),
          ("rider,driver", 2614),
          ("rider,driver,tip_history", 3629))
      else
        fail("Only Spark 3 and Spark 2 are currently supported")

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
    if (HoodieSparkUtils.isSpark3)
      Array(
        ("rider", 2452),
        ("rider,driver", 2552),
        ("rider,driver,tip_history", 3517))
    else if (HoodieSparkUtils.isSpark2)
      Array(
        ("rider", 2595),
        ("rider,driver", 2735),
        ("rider,driver,tip_history", 3750))
    else
      fail("Only Spark 3 and Spark 2 are currently supported")

    // Stats for the reads fetching _all_ columns (note, how amount of bytes read
    // is invariant of the # of columns)
    val fullColumnsReadStats: Array[(String, Long)] =
    if (HoodieSparkUtils.isSpark3)
      Array(
        ("rider", 14167),
        ("rider,driver", 14167),
        ("rider,driver,tip_history", 14167))
    else if (HoodieSparkUtils.isSpark2)
    // TODO re-enable tests (these tests are very unstable currently)
      Array(
        ("rider", -1),
        ("rider,driver", -1),
        ("rider,driver,tip_history", -1))
    else
      fail("Only Spark 3 and Spark 2 are currently supported")

    // Test MOR / Snapshot / Skip-merge
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL, projectedColumnsReadStats)

    // Test MOR / Snapshot / Payload-combine (using non-standard Record Payload)
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL, fullColumnsReadStats)
  }

  // TODO add test for incremental query of the table with logs
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
      if (HoodieSparkUtils.isSpark3)
        Array(
          ("rider", 4219),
          ("rider,driver", 4279),
          ("rider,driver,tip_history", 5186))
      else if (HoodieSparkUtils.isSpark2)
        Array(
          ("rider", 4430),
          ("rider,driver", 4530),
          ("rider,driver,tip_history", 5487))
      else
        fail("Only Spark 3 and Spark 2 are currently supported")

    val incrementalOpts: Map[String, String] = Map(
      DataSourceReadOptions.BEGIN_INSTANTTIME.key -> "001"
    )

    // Test MOR / Incremental / Skip-merge
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL,
      projectedColumnsReadStats, incrementalOpts)

    // Test MOR / Incremental / Payload-combine
    runTest(tableState, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL,
      projectedColumnsReadStats, incrementalOpts)
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

    val ds = new DefaultSource()
    val relation: HoodieBaseRelation = ds.createRelation(spark.sqlContext, readOpts).asInstanceOf[HoodieBaseRelation]

    for ((columnListStr, expectedBytesRead) <- expectedStats) {
      val targetColumns = columnListStr.split(",")

      println(s"Running test for $tablePath / $queryType / $mergeType / $columnListStr")

      val (rows, bytesRead) = measureBytesRead { () =>
        val rdd = relation.buildScan(targetColumns, Array.empty).asInstanceOf[HoodieUnsafeRDD]
        HoodieUnsafeRDDUtils.collect(rdd)
      }

      val targetRecordCount = tableState.targetRecordCount;
      val targetUpdatedRecordsRatio = tableState.targetUpdatedRecordsRatio

      val expectedRecordCount =
        if (DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL.equals(mergeType)) targetRecordCount * (1 + targetUpdatedRecordsRatio)
        else targetRecordCount

      assertEquals(expectedRecordCount, rows.length)
      if (expectedBytesRead != -1) {
        assertEquals(expectedBytesRead, bytesRead)
      } else {
        logWarning(s"Not matching bytes read ($bytesRead)")
      }

      val readColumns = targetColumns ++ relation.mandatoryFields
      val (_, projectedStructType, _) = HoodieSparkUtils.getRequiredSchema(tableState.schema, readColumns)

      val row: InternalRow = rows.take(1).head

      // This check is mostly about making sure InternalRow deserializes properly into projected schema
      val deserializedColumns = row.toSeq(projectedStructType)
      assertEquals(readColumns.length, deserializedColumns.size)
    }
  }

  private def bootstrapTable(path: String,
                             tableType: String,
                             recordCount: Int,
                             opts: Map[String, String],
                             populateMetaFields: Boolean,
                             dataGenOpt: Option[HoodieTestDataGenerator] = None): (List[HoodieRecord[_]], Schema) = {
    val dataGen = dataGenOpt.getOrElse(new HoodieTestDataGenerator(0x12345))

    // Bulk Insert Operation
    val schema =
      if (populateMetaFields) HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS
      else HoodieTestDataGenerator.AVRO_SCHEMA

    val records = dataGen.generateInserts("001", recordCount)
    val inputDF: Dataset[Row] = toDataset(records, HoodieTestDataGenerator.AVRO_SCHEMA)

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
                                dataGenOpt: Option[HoodieTestDataGenerator] = None): (List[HoodieRecord[_]], Schema) = {
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
      val inputDF = toDataset(updatedRecords, HoodieTestDataGenerator.AVRO_SCHEMA)

      inputDF.write.format("org.apache.hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .option(HoodieTableConfig.POPULATE_META_FIELDS.key, populateMetaFields.toString)
        .mode(SaveMode.Append)
        .save(path)

      (updatedRecords.asScala.toList ++ insertedRecords.drop(updatesCount), schema)
    }
  }

  def measureBytesRead[T](f: () => T): (T, Int) = {
    // Init BenchmarkCounter to report number of bytes actually read from the Block
    BenchmarkCounter.initCounterFromReporter(HadoopMapRedUtils.createTestReporter, fs.getConf)
    val r = f.apply()
    val bytesRead = BenchmarkCounter.getBytesRead.toInt
    (r, bytesRead)
  }

  case class TableState(path: String, schema: Schema, targetRecordCount: Long, targetUpdatedRecordsRatio: Double)
}
