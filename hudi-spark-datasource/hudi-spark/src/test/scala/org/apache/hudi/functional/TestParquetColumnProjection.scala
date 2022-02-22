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
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, DefaultSource, HoodieBaseRelation, HoodieSparkUtils, HoodieUnsafeRDD}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.testutils.{HadoopMapRedUtils, HoodieTestDataGenerator}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.NonpartitionedKeyGenerator
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.parquet.hadoop.util.counters.BenchmarkCounter
import org.apache.spark.HoodieUnsafeRDDUtils
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.apache.spark.sql.catalyst.InternalRow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Test}

import scala.collection.JavaConverters._

@Tag("functional")
class TestParquetColumnProjection extends SparkClientFunctionalTestHarness {

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @Test
  def testMORSnapshotRelation(): Unit = {
    val defaultOpts: Map[String, String] = commonOpts ++ Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      // NOTE: It's critical that we use non-partitioned table, since the way we track amount of bytes read
      //       is not robust, and works most reliably only when we read just a single file. As such, making table
      //       non-partitioned makes it much more likely just a single file will be written
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> classOf[NonpartitionedKeyGenerator].getName
    )

    // Test routine
    def runTest(queryType: String, mergeType: String, expectedStats: Array[(String, Long)], tableState: TableState): Unit = {
      val tablePath = tableState.path
      val readOpts = defaultOpts ++ Map(
        "path" -> tablePath,
        DataSourceReadOptions.QUERY_TYPE.key -> queryType,
        DataSourceReadOptions.REALTIME_MERGE.key -> mergeType
      )

      println(s"Running test for $tablePath / $queryType / $mergeType")

      val ds = new DefaultSource()
      val relation: HoodieBaseRelation = ds.createRelation(spark.sqlContext, readOpts).asInstanceOf[HoodieBaseRelation]

      for ((columnListStr, expectedBytesRead) <- expectedStats) {
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

      // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
      // increases along w/ the # of columns)
      val projectedColumnsReadStats: Array[(String, Long)] = Array(
        ("rider", 2452),
        ("rider,driver", 2552),
        ("rider,driver,tip_history", 3517)
      )

      // Stats for the reads fetching _all_ columns (note, how amount of bytes read
      // is invariant of the # of columns)
      val fullColumnsReadStats: Array[(String, Long)] = Array(
        ("rider", 14666),
        ("rider,driver", 14666),
        ("rider,driver,tip_history", 14665)
      )

      // Test MOR / Snapshot / Skip-merge
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
        mergeType = DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL,
        expectedStats = projectedColumnsReadStats,
        tableState = tableState
      )

      // Test MOR / Snapshot / Payload-combine
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
        mergeType = DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL,
        expectedStats = fullColumnsReadStats,
        tableState = tableState
      )

      // Test MOR / Read Optimized
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL,
        mergeType = "null",
        expectedStats = projectedColumnsReadStats,
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

      // Stats for the reads fetching only _projected_ columns (note how amount of bytes read
      // increases along w/ the # of columns)
      val projectedColumnsReadStats: Array[(String, Long)] = Array(
        ("rider", 2452),
        ("rider,driver", 2552),
        ("rider,driver,tip_history", 3517)
      )

      // Test MOR / Snapshot / Skip-merge
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
        mergeType = DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL,
        expectedStats = projectedColumnsReadStats,
        tableState = tableState
      )

      // Test MOR / Snapshot / Payload-combine
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
        mergeType = DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL,
        expectedStats = projectedColumnsReadStats,
        tableState = tableState
      )

      // Test MOR / Read Optimized
      runTest(
        queryType = DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL,
        mergeType = "null",
        expectedStats = projectedColumnsReadStats,
        tableState = tableState
      )
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
    val inputDF: Dataset[Row] = toDataset(records)

    inputDF.write.format("org.apache.hudi")
      .options(opts)
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
      val inputDF = toDataset(updatedRecords)

      inputDF.write.format("org.apache.hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
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
}
