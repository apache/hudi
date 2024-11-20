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

import org.apache.hudi.DataSourceWriteOptions.{RECORD_MERGE_MODE, RECORD_MERGE_STRATEGY_ID}
import org.apache.hudi.common.model.HoodieRecordMerger
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.storage.{HoodieStorageUtils, StorageConfiguration}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Tag
import org.junit.{After, Before, Test}

import java.nio.file.{Files, Path}

@Tag("functional")
class TestCommitTimeMergeOnCompaction extends SparkClientFunctionalTestHarness {
  var tmpDir: Path = _
  var tblPath: String = _

  override def basePath(): String = tmpDir.toAbsolutePath.toUri.toString

  @Before
  def setUp(): Unit = {
    tmpDir = Files.createTempDirectory("hudi_random")
    tblPath = basePath()
    super.runBeforeEach()
  }

  @After
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
      "hoodie.populate.meta.fields=true" -> "false",
      "hoodie.compaction.payload.class" -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
      "hoodie.datasource.write.payload.class" -> "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload",
      RECORD_MERGE_STRATEGY_ID.key -> HoodieRecordMerger.OVERWRITE_MERGE_STRATEGY_UUID,
      RECORD_MERGE_MODE.key -> "OVERWRITE_WITH_LATEST")

    val serviceOpt: Map[String, String] = Map(
      "hoodie.table.services.enabled" -> "true",
      "hoodie.compact.inline" -> "true",
      "hoodie.compact.inline.max.delta.commits" -> "3",
      "hoodie.parquet.small.file.limit" -> "0")
    val opt = tableOpt ++ serviceOpt

    val schema = new StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("ts", LongType, nullable = true)
    ))

    // First batch.
    val data1 = Seq(
      Row("id1", "name1", 2L),
      Row("id2", "name2", 2L),
      Row("id3", "name3", 2L))
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(data1, 2), schema)
    df1.write
      .format("hudi")
      .options(opt)
      .mode(SaveMode.Append)
      .save(tblPath)

    val storageConfiguration = new HadoopStorageConfiguration(false)
    var metaClient = HoodieTableMetaClient.builder()
      .setBasePath(tblPath).setStorage(
        HoodieStorageUtils.getStorage(storageConfiguration)).build()
    System.out.println(metaClient.getActiveTimeline.getInstants)

    // Second batch.
    val data2 = Seq(
      Row("id1", "name11", 3L),
      Row("id2", "name22", 3L),
      Row("id3", "name33", 3L))
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2, 2), schema)
    df2.write
      .format("hudi")
      .options(opt)
      .mode(SaveMode.Append)
      .save(tblPath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    System.out.println(metaClient.getActiveTimeline.getInstants)

    // Third batch.
    val data3 = Seq(
      Row("id1", "name111", 1L),
      Row("id2", "name222", 1L),
      Row("id3", "name333", 1L))
    val df3 = spark.createDataFrame(spark.sparkContext.parallelize(data3, 2), schema)
    df3.write
      .format("hudi")
      .options(opt)
      .mode(SaveMode.Append)
      .save(tblPath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    System.out.println(metaClient.getActiveTimeline.getInstants)

    assertFalse(metaClient.getCommitTimeline.getCommitsAndCompactionTimeline.getInstants.isEmpty)

    val readDf = spark.read.format("hudi")
      .option("hoodie.schema.on.read.enable", "true")
      .options(opt)
      .load(tblPath)
    val actual = readDf.select("id",  "name", "ts")
    val r = actual.except(df3)
    val l = df3.except(actual)
    assertTrue(r.isEmpty && l.isEmpty)
  }
}
