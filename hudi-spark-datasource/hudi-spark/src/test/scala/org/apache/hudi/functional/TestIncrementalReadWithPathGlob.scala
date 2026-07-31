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
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.{recordsToStrings, DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import scala.collection.JavaConverters._

/**
 * Coverage for the {@code hoodie.datasource.read.incr.path.glob} file-slice filtering branch of the
 * batch incremental relations {@code MergeOnReadIncrementalRelationV1} (table version 6) and
 * {@code MergeOnReadIncrementalRelationV2} (table version 8). Both the COW and the MOR batch
 * incremental read routes build a {@code HoodieIncrementalFileIndex} backed by these relations (see
 * {@code HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1/V2} and the MOR factories), so the
 * COW and MOR cases together exercise {@code filterFileSlices} with {@code includeLogFiles} both
 * off and on, and the table version selects V1 vs V2 in {@code DefaultSource}.
 *
 * Existing glob coverage ({@code TestCOWDataSourceStorage}) exercises only COW at the default table
 * version and asserts counts alone; this pins the per-partition result of the glob (including the
 * glob-matches-nothing empty branch) across both table versions and both table types.
 */
class TestIncrementalReadWithPathGlob extends HoodieSparkClientTestBase {

  private var spark: SparkSession = _
  // generateInsertsForPartition sets the record's partition_path field to the given partition.
  private val firstPartition = DEFAULT_FIRST_PARTITION_PATH // 2016/03/15
  private val secondPartition = DEFAULT_SECOND_PARTITION_PATH // 2015/03/16
  private val numFirst = 8
  private val numSecond = 5

  @BeforeEach override def setUp(): Unit = {
    setTableName("hoodie_test")
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    spark = null
    cleanupResources()
  }

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,6",
    "COPY_ON_WRITE,8",
    "MERGE_ON_READ,6",
    "MERGE_ON_READ,8"))
  def testIncrementalPathGlobPartitionFiltering(tableType: String, tableVersion: Int): Unit = {
    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.upsert.shuffle.parallelism" -> "2",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> tableVersion.toString,
      HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false")

    // First commit writes only the first partition, second commit only the second partition, so the
    // glob boundary lines up with the commit boundary and expected counts are deterministic.
    writeBatch("001", numFirst, firstPartition, opts, SaveMode.Overwrite)
    writeBatch("002", numSecond, secondPartition, opts, SaveMode.Append)

    val metaClient = createMetaClient(spark, basePath)
    assertEquals(tableVersion, metaClient.getTableConfig.getTableVersion.versionCode(),
      "table should be written at the requested version to select the V1/V2 relation")

    // Without a glob the incremental span returns every record from both partitions.
    assertPartitionCounts(incrementalRead(None), Map(firstPartition -> numFirst, secondPartition -> numSecond))

    // Glob restricted to the first partition returns only its records.
    assertPartitionCounts(incrementalRead(Some("/2016/*/*/*")), Map(firstPartition -> numFirst))

    // Glob restricted to the second partition returns only its records.
    assertPartitionCounts(incrementalRead(Some("/2015/*/*/*")), Map(secondPartition -> numSecond))

    // Glob that matches no partition exercises the empty-result branch of the relation.
    assertEquals(0, incrementalRead(Some("/9999/*/*/*")).count(),
      "a glob matching no partition path must yield an empty result")
  }

  private def writeBatch(instant: String, n: Int, partition: String,
                         opts: Map[String, String], mode: SaveMode): Unit = {
    val records = recordsToStrings(dataGen.generateInsertsForPartition(instant, n, partition)).asScala.toList
    val df = spark.read.json(spark.sparkContext.parallelize(records, 2))
    df.write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(mode)
      .save(basePath)
  }

  private def incrementalRead(pathGlob: Option[String]): DataFrame = {
    var reader = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "000")
    pathGlob.foreach(g => reader = reader.option(DataSourceReadOptions.INCR_PATH_GLOB.key, g))
    reader.load(basePath)
  }

  private def assertPartitionCounts(df: DataFrame, expected: Map[String, Int]): Unit = {
    val actual = df.groupBy("_hoodie_partition_path").count().collect()
      .map(row => row.getString(0) -> row.getLong(1).toInt).toMap
    assertEquals(expected, actual, "incremental path glob returned unexpected per-partition counts")
  }
}
