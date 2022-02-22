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

import org.apache.avro.Schema
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.testutils.{HadoopMapRedUtils, HoodieTestDataGenerator}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.functional.TestMORDataSourceStorage.testParams
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, DefaultSource, HoodieBaseRelation, HoodieDataSourceHelpers, HoodieSparkUtils, HoodieUnsafeRDD}
import org.apache.log4j.LogManager
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter
import org.apache.spark.HoodieUnsafeRDDUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{col, lit}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, MethodSource, ValueSource}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._


@Tag("functional")
class TestMORDataSourceStorage extends SparkClientFunctionalTestHarness {

  private val log = LogManager.getLogger(classOf[TestMORDataSourceStorage])

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  val verificationCol: String = "driver"
  val updatedVerificationVal: String = "driver_update"

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testMergeOnReadStorage(isMetadataEnabled: Boolean) {
    val dataGen = new HoodieTestDataGenerator()
    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    // Bulk Insert Operation
    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
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
    val insertCommitTimes = hudiRODF1.select("_hoodie_commit_time").distinct().collectAsList().map(r => r.getString(0)).toList
    assertEquals(List(insertCommitTime), insertCommitTimes)

    // Upsert operation without Hudi metadata columns
    val records2 = recordsToStrings(dataGen.generateUniqueUpdates("002", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)

    // Read Snapshot query
    val updateCommitTime = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val hudiSnapshotDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)

    val updateCommitTimes = hudiSnapshotDF2.select("_hoodie_commit_time").distinct().collectAsList().map(r => r.getString(0)).toList
    assertEquals(List(updateCommitTime), updateCommitTimes)

    // Upsert based on the written table with Hudi metadata columns
    val verificationRowKey = hudiSnapshotDF2.limit(1).select("_row_key").first.getString(0)
    val inputDF3 = hudiSnapshotDF2.filter(col("_row_key") === verificationRowKey).withColumn(verificationCol, lit(updatedVerificationVal))

    inputDF3.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .mode(SaveMode.Append)
      .save(basePath)

    val hudiSnapshotDF3 = spark.read.format("hudi")
      .option(HoodieMetadataConfig.ENABLE.key, isMetadataEnabled)
      .load(basePath)
    assertEquals(100, hudiSnapshotDF3.count())
    assertEquals(updatedVerificationVal, hudiSnapshotDF3.filter(col("_row_key") === verificationRowKey).select(verificationCol).first.getString(0))
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

    val records = dataGen.generateInserts("001", recordCount).toList
    val inputDF: Dataset[Row] = toDataset(records)

    inputDF.write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.TABLE_TYPE.key, tableType)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(path)

    (records, schema)
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
      val updatedRecords = dataGen.generateUpdates("002", recordsToUpdate).asScala.toList

      // Step 2: Update M records out of those (t/h update)
      val inputDF = toDataset(updatedRecords)

      inputDF.write.format("org.apache.hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(path)

      (updatedRecords ++ insertedRecords.drop(updatesCount), schema)
    }
  }

  @Test
  def testProperProjection(): Unit = {
    val defaultOpts: Map[String, String] = commonOpts ++ Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      // NOTE: It's critical that we use non-partitioned table, since the way we track amount of bytes read
      //       is not robust, and works most reliably only when we read just a single file. As such, making table
      //       non-partitioned makes it much more likely just a single file will be written
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getName
    )

    // Test routine
    def runTest(queryType: String, mergeType: String, inputs: Array[(String, Long)], tableState: TableState): Unit = {
      val tablePath = tableState.path
      val readOpts = defaultOpts ++ Map(
        "path" -> tablePath,
        DataSourceReadOptions.QUERY_TYPE.key -> queryType,
        DataSourceReadOptions.REALTIME_MERGE.key -> mergeType
      )

      println(s"Running test for $tablePath / $queryType / $mergeType")

      val ds = new DefaultSource()
      val relation: HoodieBaseRelation = ds.createRelation(spark.sqlContext, readOpts).asInstanceOf[HoodieBaseRelation]

      for ((columnListStr, expectedBytesRead) <- inputs) {
        val targetColumns = columnListStr.split(",")

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
        assertEquals(expectedBytesRead, bytesRead)

        val readColumns = targetColumns ++ relation.mandatoryColumns
        val (_, projectedStructType) = HoodieSparkUtils.getRequiredSchema(tableState.schema, readColumns)

        val row: InternalRow = rows.take(1).head

        // This check is mostly about making sure InternalRow deserializes properly into projected schema
        val deserializedColumns = row.toSeq(projectedStructType)
        assertEquals(readColumns.length, deserializedColumns.size)
      }
    }

    case class TableState(path: String, schema: Schema, targetRecordCount: Long, targetUpdatedRecordsRatio: Double)


    //
    // Test #1: MOR table w/ Delta Logs
    //
    {
      val tablePath = s"$basePath/mor-with-logs"
      val targetRecordsCount = 100
      val targetUpdatedRecordsRatio = 0.5

      val (_, schema) = bootstrapMORTable(tablePath, targetRecordsCount, targetUpdatedRecordsRatio, defaultOpts, populateMetaFields = true)
      val tableState = TableState(tablePath, schema, targetRecordsCount, targetUpdatedRecordsRatio)


      // NOTE: Values for the amount of bytes read should be stable, and not change a lot
      //       since file format layout on disk is very stable
      val inputs: Array[(String, Long)] = Array(
        ("rider", 2452),
        ("rider,driver", 2552),
        ("rider,driver,tip_history", 3517)
      )

      // Test MOR / Snapshot / Skip-merge
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
        mergeType = DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL,
        inputs = inputs,
        tableState = tableState
      )

      // Test MOR / Read Optimized
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL,
        mergeType = "null",
        inputs = inputs,
        tableState = tableState
      )
    }

    //
    // Test #2: MOR table w/ NO Delta Logs
    //
    {
      val tablePath = s"$basePath/mor-no-logs"
      val targetRecordsCount = 100
      val targetUpdatedRecordsRatio = 0.0

      val (_, schema) = bootstrapMORTable(tablePath, targetRecordsCount, targetUpdatedRecordsRatio, defaultOpts, populateMetaFields = true)
      val tableState = TableState(tablePath, schema, targetRecordsCount, targetUpdatedRecordsRatio)

      //
      // Test #1: MOR table w/ Delta Logs
      //

      // NOTE: Values for the amount of bytes read should be stable, and not change a lot
      //       since file format layout on disk is very stable
      val inputs: Array[(String, Long)] = Array(
        ("rider", 2452),
        ("rider,driver", 2552),
        ("rider,driver,tip_history", 3517)
      )

      // Test MOR / Snapshot / Skip-merge
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
        mergeType = DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL,
        inputs = inputs,
        tableState = tableState
      )

      // Test MOR / Snapshot / Payload-combine
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
        mergeType = DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL,
        inputs = inputs,
        tableState = tableState
      )

      // Test MOR / Read Optimized
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL,
        mergeType = "null",
        inputs = inputs,
        tableState = tableState
      )
    }
  }

  def measureBytesRead[T](f: () => T): (T, Int) = {
    // Init BenchmarkCounter to report number of bytes actually read from the Block
    BenchmarkCounter.initCounterFromReporter(HadoopMapRedUtils.createTestReporter, fs.getConf)
    val r = f.apply()
    val bytesRead = BenchmarkCounter.getBytesRead.toInt
    (r, bytesRead)
  }
}

object TestMORDataSourceStorage {
  def testParams(): Array[Array[String]] = Array(
    Array(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL),
    Array(DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, null)
  )
}
