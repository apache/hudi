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

package org.apache.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.DataSourceWriteOptions.{ENABLE_MERGE_INTO_PARTIAL_UPDATES, ENABLE_ROW_WRITER, RECORD_MERGE_IMPL_CLASSES}
import org.apache.hudi.common.config.HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS
import org.apache.hudi.common.config.HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT
import org.apache.hudi.common.table.HoodieTableConfig.LOG_FILE_FORMAT
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.HoodieCleanConfig.AUTO_CLEAN
import org.apache.hudi.config.HoodieClusteringConfig.{INLINE_CLUSTERING, INLINE_CLUSTERING_MAX_COMMITS}
import org.apache.hudi.config.HoodieCompactionConfig.{INLINE_COMPACT, INLINE_COMPACT_NUM_DELTA_COMMITS, PARQUET_SMALL_FILE_LIMIT}
import org.apache.hudi.config.HoodieWriteConfig.TABLE_SERVICES_ENABLED
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.storage.{HoodieStorage, HoodieStorageUtils, StorageConfiguration, StoragePath}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.{After, Before, Test}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

/**
 * Test class for compaction using file group readers in Hudi.
 */
class TestCompactionWithFileGroupReader extends SparkClientFunctionalTestHarness {
  private val log: Logger = LoggerFactory.getLogger(classOf[TestCompactionWithFileGroupReader])
  private var storage: HoodieStorage = _
  private var tempDirectory: Path = _
  private var tablePath: String = _
  private var metaClient: HoodieTableMetaClient = _

  override def basePath(): String = tempDirectory.toAbsolutePath.toUri.toString

  @Before
  def setUp(): Unit = {
    log.info("Setting up the test environment.")
    tempDirectory = Files.createTempDirectory("hudi_test")
    super.runBeforeEach()
    val storageConfig: StorageConfiguration[Configuration] = new HadoopStorageConfiguration(true)
    tablePath = basePath()
    storage = HoodieStorageUtils.getStorage(new StoragePath(tablePath), storageConfig)
  }

  @After
  def tearDown(): Unit = {
    log.info("Tearing down the test environment.")
    super.closeFileSystem()
  }

  @Test
  def testCompactionWithFileGroupReader(): Unit = {
    val tableOptions = Map(
      "hoodie.datasource.write.table.name" -> "test_table",
      "hoodie.datasource.write.table.type" -> "MERGE_ON_READ",
      "hoodie.datasource.write.recordkey.field" -> "id",
      "hoodie.datasource.write.precombined.field" -> "ts",
      "hoodie.datasource.write.partitionpath.field" -> "name",
      "hoodie.populate.meta.fields" -> "false",
      ENABLE_ROW_WRITER.key -> "true",
      LOG_FILE_FORMAT.key -> "parquet",
      LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet",
      RECORD_MERGE_IMPL_CLASSES.key -> classOf[DefaultSparkRecordMerger].getName,
      MERGE_USE_RECORD_POSITIONS.key -> "false")
    val serviceOptions = Map(
      TABLE_SERVICES_ENABLED.key -> "true",
      INLINE_COMPACT.key -> "true",
      INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "2",
      PARQUET_SMALL_FILE_LIMIT.key -> "0",
      AUTO_CLEAN.key -> "false",
      INLINE_CLUSTERING.key -> "false",
      INLINE_CLUSTERING_MAX_COMMITS.key -> "1")
    val schema = new StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("ts", LongType, nullable = true)
    ))

    var options = tableOptions ++ serviceOptions
    for (partialUpdateEnabled <- List("false", "true")) {
      options += (ENABLE_MERGE_INTO_PARTIAL_UPDATES.key -> partialUpdateEnabled)

      // Write initial dataset
      val df1 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row("id1", "name1", 1L),
        Row("id2", "name1", 2L),
        Row("id3", "name1", 3L)
      )), schema)
      df1.write.format("hudi").options(options).mode(SaveMode.Overwrite).save(tablePath)

      // Write additional dataset
      val df2 = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
        Row("id1", "name1", 4L),
        Row("id2", "name1", 5L),
        Row("id3", "name1", 6L)
      )), schema)
      df2.write.format("hudi").options(options).mode(SaveMode.Append).save(tablePath)

      // Validate the results
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tablePath)
        .setStorage(storage)
        .build()

      val instants = metaClient.getActiveTimeline.getInstants.asScala.toList
      assertTrue(instants.exists(i => i.getAction == "commit" && i.isCompleted))

      val finalData = spark.read.format("hudi").load(tablePath)
      val finalDf = finalData.select("id", "name", "ts").sort("id")
      finalDf.show(false)
      val expectedDf = df2.sort("id")
      expectedDf.show(false)

      assertEquals(3, finalData.count())
      assertTrue(finalDf.except(expectedDf).isEmpty)
      assertTrue(expectedDf.except(finalDf).isEmpty)
    }
  }
}
