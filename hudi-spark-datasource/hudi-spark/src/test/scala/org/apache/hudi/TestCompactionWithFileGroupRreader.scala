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
import org.apache.hudi.DataSourceWriteOptions.{ENABLE_ROW_WRITER, RECORD_MERGE_IMPL_CLASSES}
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
import org.junit.{After, Before, Test}
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.asScalaBufferConverter

class TestCompactionWithFileGroupRreader extends SparkClientFunctionalTestHarness {
  val LOG: Logger = LoggerFactory.getLogger(classOf[TestCompactionWithFileGroupRreader])
  var storage: HoodieStorage = _
  var tmpDir: Path = _
  var tblPath: String = _
  var metaClient: HoodieTableMetaClient = _
  var hasDeleted: Boolean = _

  override def basePath(): String = tmpDir.toAbsolutePath.toUri.toString

  @Before
  def setUp(): Unit = {
    tmpDir = Files.createTempDirectory("hudi_random")
    super.runBeforeEach()
    val sc: StorageConfiguration[Configuration] = new HadoopStorageConfiguration(true)
    tblPath = basePath()
    storage = HoodieStorageUtils.getStorage(new StoragePath(tblPath), sc)
    hasDeleted = false
  }

  @After
  def tearDown(): Unit = {
    super.closeFileSystem()
  }

  @Test
  def testCompactionUsingFileGroupReader(): Unit = {
    val tableOpt: Map[String, String] = Map(
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
    )

    val serviceOpt: Map[String, String] = Map(
      TABLE_SERVICES_ENABLED.key -> "true",
      INLINE_COMPACT.key -> "true",
      INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "2",
      PARQUET_SMALL_FILE_LIMIT.key -> "0",
      AUTO_CLEAN.key -> "false",
      INLINE_CLUSTERING.key -> "false",
      INLINE_CLUSTERING_MAX_COMMITS.key -> "1"
    )
    val opt = tableOpt ++ serviceOpt

    val schema = new StructType(Array(
      StructField("id", StringType, nullable = true),
      StructField("name", StringType, nullable = true),
      StructField("ts", LongType, nullable = true)
    ))

    val data1 = Seq(
      Row("id1", "name1", 1L),
      Row("id2", "name1", 2L),
      Row("id3", "name1", 3L))
    val df1 = spark.createDataFrame(
      spark.sparkContext.parallelize(data1, 2), schema)
    df1.write
      .format("hudi")
      .option("hoodie.table.log.file.format", "parquet")
      .options(opt)
      .mode(SaveMode.Overwrite)
      .save(tblPath)

    val data2 = Seq(
      Row("id1", "name1", 4L),
      Row("id2", "name1", 5L),
      Row("id3", "name1", 6L))
    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(data2, 2), schema)
    df2.write
      .format("hudi")
      .option("hoodie.clustering.async.enabled", "true")
      .options(opt)
      .mode(SaveMode.Append)
      .save(tblPath)

    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(tblPath).setStorage(storage).build()
    val instants = metaClient.getActiveTimeline.getInstants.asScala.toList
    assertTrue(instants.exists(i => i.getAction.equals("commit") && i.isCompleted))

    val df = spark.read.format("hudi").load(tblPath)
    assertEquals(3, df.count())
    val d1 = df.except(df2)
    val d2 = df2.except(df)
    assertTrue(d1.isEmpty && d2.isEmpty)
  }
}
