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
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestDataGenerator.{recordsToStrings, DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.{List => JList}

import scala.collection.JavaConverters._

/**
 * Coverage for the {@code hoodie.datasource.read.incr.path.glob} file-slice filtering branch of the
 * batch incremental relations {@code MergeOnReadIncrementalRelationV1} (table version 6) and
 * {@code MergeOnReadIncrementalRelationV2} (table version 8). Both the COW and the MOR batch
 * incremental read routes build a {@code HoodieIncrementalFileIndex} backed by these relations (see
 * {@code HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1/V2} and the MOR factories), and the
 * table version selects V1 vs V2 in {@code DefaultSource}. The COW arms cover the base-file path of
 * {@code filterFileSlices}, since the COW factory builds the file index with
 * {@code includeLogFiles = false}. The MOR arms write with the in-memory index, which can index log
 * files, so their inserts land in log-only file slices; those arms therefore exercise the
 * {@code getLatestLogFile} fallback of {@code filterFileSlices} and, because the MOR factory builds
 * the file index with {@code includeLogFiles = true}, the log-file branch of
 * {@code HoodieIncrementalFileIndex.inputFiles}.
 *
 * Existing glob coverage ({@code TestCOWDataSourceStorage}, plus {@code TestDataSourceForBootstrap}
 * for bootstrapped COW) exercises only COW at the default table
 * version and asserts counts alone; this pins the per-partition result of the glob (including the
 * glob-matches-nothing empty branch) across both table versions and both table types, the merged
 * value of an upserted record inside the glob, and the file paths reported by {@code inputFiles}.
 */
class TestIncrementalReadWithPathGlob extends HoodieSparkClientTestBase {

  private var spark: SparkSession = _
  // generateInsertsForPartition sets the record's partition_path field to the given partition.
  private val firstPartition = DEFAULT_FIRST_PARTITION_PATH // 2016/03/15
  private val secondPartition = DEFAULT_SECOND_PARTITION_PATH // 2015/03/16
  private val numFirst = 8
  private val numSecond = 5
  private val numUpdated = 3
  // generateUpdates stamps the rider field with "rider-" + the seed it is given.
  private val updatedRider = "rider-003"

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
    val baseOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.upsert.shuffle.parallelism" -> "2",
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "_row_key",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition_path",
      HoodieTableConfig.ORDERING_FIELDS.key -> "timestamp",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> tableVersion.toString,
      HoodieWriteConfig.AUTO_UPGRADE_VERSION.key -> "false")
    // The default SIMPLE index cannot index log files, so MOR inserts would land in base files and
    // every file slice would carry a base file. The in-memory index can, which keeps the MOR arms
    // on log-only file slices and drives the log-file branches of the relation and the file index.
    val opts = if (DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL.equals(tableType)) {
      baseOpts + (HoodieIndexConfig.INDEX_TYPE.key -> IndexType.INMEMORY.toString)
    } else {
      baseOpts
    }

    // First commit writes only the first partition, second commit only the second partition, so the
    // glob boundary lines up with the commit boundary and expected counts are deterministic.
    val firstBatch = writeBatch("001", numFirst, firstPartition, opts, SaveMode.Overwrite)
    writeBatch("002", numSecond, secondPartition, opts, SaveMode.Append)
    // Third commit upserts a subset of the first batch. Updates keep the record key and partition,
    // so the per-partition counts must stay the same and the updated rows must be merged in place.
    writeUpdateBatch("003", firstBatch.subList(0, numUpdated), opts)

    val metaClient = createMetaClient(spark, basePath)
    assertEquals(tableVersion, metaClient.getTableConfig.getTableVersion.versionCode(),
      "table should be written at the requested version to select the V1/V2 relation")

    // Without a glob the incremental span returns every record from both partitions.
    val allDf = incrementalRead(None)
    assertPartitionCounts(allDf, Map(firstPartition -> numFirst, secondPartition -> numSecond))
    val allInputFiles = allDf.inputFiles
    if (DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL.equals(tableType)) {
      // Guards the coverage this test is here for: if MOR inserts ever stop landing in log files the
      // getLatestLogFile fallback of filterFileSlices and the log branch of inputFiles go untested.
      assertTrue(allInputFiles.forall(_.contains(".log.")),
        "MOR file slices must be log only under the in-memory index: " + allInputFiles.mkString(","))
    }
    assertTrue(allInputFiles.exists(_.contains(firstPartition)),
      "without a glob inputFiles must report files from the first partition: " + allInputFiles.mkString(","))
    assertTrue(allInputFiles.exists(_.contains(secondPartition)),
      "without a glob inputFiles must report files from the second partition: " + allInputFiles.mkString(","))

    // Glob restricted to the first partition returns only its records.
    val firstDf = incrementalRead(Some("/2016/*/*/*"))
    assertPartitionCounts(firstDf, Map(firstPartition -> numFirst))
    assertEquals(numUpdated, firstDf.filter(s"rider = '$updatedRider'").count(),
      "the upserted records must be merged into the glob result, not duplicated or dropped")
    val firstInputFiles = firstDf.inputFiles
    assertTrue(firstInputFiles.nonEmpty, "a glob matching the first partition must report input files")
    assertTrue(firstInputFiles.forall(_.contains(firstPartition)),
      "every file reported for the first-partition glob must live under it: " + firstInputFiles.mkString(","))

    // Glob restricted to the second partition returns only its records.
    val secondDf = incrementalRead(Some("/2015/*/*/*"))
    assertPartitionCounts(secondDf, Map(secondPartition -> numSecond))
    val secondInputFiles = secondDf.inputFiles
    assertTrue(secondInputFiles.nonEmpty, "a glob matching the second partition must report input files")
    assertTrue(secondInputFiles.forall(_.contains(secondPartition)),
      "every file reported for the second-partition glob must live under it: " + secondInputFiles.mkString(","))

    // Glob that matches no partition exercises the empty-result branch of the relation.
    val emptyDf = incrementalRead(Some("/9999/*/*/*"))
    assertEquals(0, emptyDf.count(),
      "a glob matching no partition path must yield an empty result")
    assertEquals(0, emptyDf.inputFiles.length,
      "a glob matching no partition path must report no input files")
  }

  /**
   * Writes an insert batch and returns the generated records so that later batches can update them.
   * The seed only feeds the generated field values (rider-001, driver-001, ...); it does not control
   * the commit time of the write.
   */
  private def writeBatch(seed: String, n: Int, partition: String,
                         opts: Map[String, String], mode: SaveMode): JList[HoodieRecord[_]] = {
    val records = dataGen.generateInsertsForPartition(seed, n, partition)
    write(records, opts, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL, mode)
    records
  }

  private def writeUpdateBatch(seed: String, baseRecords: JList[HoodieRecord[_]],
                               opts: Map[String, String]): Unit = {
    write(dataGen.generateUpdates(seed, baseRecords), opts,
      DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL, SaveMode.Append)
  }

  private def write(records: JList[HoodieRecord[_]], opts: Map[String, String],
                    operation: String, mode: SaveMode): Unit = {
    val rows = recordsToStrings(records).asScala.toList
    val df = spark.read.json(spark.sparkContext.parallelize(rows, 2))
    df.write.format("org.apache.hudi")
      .options(opts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
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
