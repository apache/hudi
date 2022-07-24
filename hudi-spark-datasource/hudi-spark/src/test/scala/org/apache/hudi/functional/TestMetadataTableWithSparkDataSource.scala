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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{Tag, Test}

import scala.collection.JavaConverters._

@Tag("functional")
class TestMetadataTableWithSparkDataSource extends SparkClientFunctionalTestHarness {

  val hudi = "org.apache.hudi"
  var commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "2",
    "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  )

  @Test
  def testReadability(): Unit = {
    val dataGen = new HoodieTestDataGenerator()

    val metadataOpts: Map[String, String] = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val combinedOpts: Map[String, String] = commonOpts ++ metadataOpts ++
      Map(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "1")

    // Insert records
    val newRecords = dataGen.generateInserts("001", 100)
    val newRecordsDF = parseRecords(recordsToStrings(newRecords).asScala)

    newRecordsDF.write.format(hudi)
      .options(combinedOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // Update records
    val updatedRecords = dataGen.generateUpdates("002", newRecords)
    val updatedRecordsDF = parseRecords(recordsToStrings(updatedRecords).asScala)

    updatedRecordsDF.write.format(hudi)
      .options(combinedOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // Files partition of MT
    val filesPartitionDF = spark.read.format(hudi).load(s"$basePath/.hoodie/metadata/files")

    // Smoke test
    filesPartitionDF.show()

    // Query w/ 0 requested columns should be working fine
    assertEquals(4, filesPartitionDF.count())

    val expectedKeys = Seq("2015/03/16", "2015/03/17", "2016/03/15", "__all_partitions__")
    val keys = filesPartitionDF.select("key")
      .collect()
      .map(_.getString(0))
      .toSeq
      .sorted

    assertEquals(expectedKeys, keys)

    // Column Stats Index partition of MT
    val colStatsDF = spark.read.format(hudi).load(s"$basePath/.hoodie/metadata/column_stats")

    // Smoke test
    colStatsDF.select("ColumnStatsMetadata").show()
  }

  private def parseRecords(records: Seq[String]) = {
    spark.read.json(spark.sparkContext.parallelize(records, 2))
  }
}
