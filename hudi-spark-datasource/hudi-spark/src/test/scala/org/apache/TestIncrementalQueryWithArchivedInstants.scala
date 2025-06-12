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

package org.apache

import org.apache.hudi.DataSourceWriteOptions.RECORD_MERGE_MODE
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.storage.HoodieStorageUtils
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.{BeforeEach, Tag, Test}
import org.junit.jupiter.api.Assertions.{assertDoesNotThrow, assertFalse}
import org.junit.jupiter.api.function.Executable

@Tag("functional")
class TestIncrementalQueryWithArchivedInstants extends SparkClientFunctionalTestHarness {
  var tblPath: String = _

  @BeforeEach
  def setUp(): Unit = {
    tblPath = basePath()
    super.runBeforeEach()
  }

  def tearDown(): Unit = {
    super.closeFileSystem()
  }

  @Test
  def testCompactionWithCommitTimeMerge(): Unit = {
    val tableOpt: Map[String, String] = Map(
      "hoodie.datasource.write.table.name" -> "test_table",
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.precombined.field" -> "ts",
      "hoodie.datasource.write.partitionpath.field" -> "name",
      "hoodie.populate.meta.fields" -> "true",
      "hoodie.compaction.payload.class" -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
      "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
      RECORD_MERGE_MODE.key -> RecordMergeMode.COMMIT_TIME_ORDERING.name,
      HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS.key -> "10"
    )

    val serviceOpt: Map[String, String] = Map(
      "hoodie.table.services.enabled" -> "true",
      "hoodie.compact.inline" -> "false",
      "hoodie.compact.inline.max.delta.commits" -> "1",
      "hoodie.parquet.small.file.limit" -> "0",
      "hoodie.clustering.inline" -> "false",
      "hoodie.keep.max.commits" -> "2",
      "hoodie.keep.min.commits" -> "1",
      "hoodie.commits.archival.batch" -> "1")
    val schema = new StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("ts", LongType, nullable = true)
    ))

    val opt = tableOpt ++ serviceOpt
    for (i <- 1L to 10L) {
      val data = Seq(Row("id1", "name1", i), Row("id2", "name2", i), Row("id3", "name3", i))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data, 2), schema)
      df.write.format("hudi").options(opt).mode(SaveMode.Append).save(tblPath)
    }

    val storageConfiguration = new HadoopStorageConfiguration(false)
    val metaClient = HoodieTableMetaClient.builder().setBasePath(tblPath)
      .setStorage(HoodieStorageUtils.getStorage(storageConfiguration)).build()
    val instants = metaClient.getArchivedTimeline().getInstants

    // There are at least one archived instants.
    assertFalse(instants.isEmpty)
    // No errors during read.
    assertDoesNotThrow(new Executable {
      def execute(): Unit = {
        spark.read.format("hudi")
          .option("hoodie.schema.on.read.enable", "true")
          .option("hoodie.datasource.query.type", "incremental")
          .option("hoodie.datasource.read.begin.instanttime", "0")
          .options(opt)
          .load(tblPath)
          .show(false)
      }
    })
  }
}
