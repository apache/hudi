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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions}
import org.apache.hudi.HoodieFileIndex.DataSkippingFailureMode
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieWriteConfig}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, BeforeEach, Tag}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.junit.jupiter.params.provider.Arguments.arguments

import scala.collection.JavaConverters._

@Tag("functional-c")
class TestLayoutOptimization extends HoodieSparkClientTestBase {
  var spark: SparkSession = _

  val sourceTableSchema =
    new StructType()
      .add("c1", IntegerType)
      .add("c2", StringType)
      .add("c3", DecimalType(9,3))
      .add("c4", TimestampType)
      .add("c5", ShortType)
      .add("c6", DateType)
      .add("c7", BinaryType)
      .add("c8", ByteType)

  val metadataOpts = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
  )

  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    "hoodie.bulkinsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD.key() -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> "partition",
    HoodieTableConfig.ORDERING_FIELDS.key() -> "timestamp",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test"
  ) ++ metadataOpts

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown() = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @MethodSource(Array("testLayoutOptimizationParameters"))
  def testLayoutOptimizationFunctional(tableType: String,
                                       clusteringAsRow: String,
                                       layoutOptimizationStrategy: String,
                                       spatialCurveCompositionStrategy: String): Unit = {
    val curveCompositionStrategy =
      Option(spatialCurveCompositionStrategy)
        .getOrElse(HoodieClusteringConfig.LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD.defaultValue())

    val targetRecordsCount = 10000
    // Bulk Insert Operation
    val records = recordsToStrings(dataGen.generateInserts("001", targetRecordsCount)).asScala.toList
    val writeDf: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records, 2))

    // If there are any failures in the Data Skipping flow, test should fail
    spark.sqlContext.setConf(DataSkippingFailureMode.configName, DataSkippingFailureMode.Strict.value);

    writeDf.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false")
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), tableType)
      // option for clustering
      .option("hoodie.parquet.small.file.limit", "0")
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "1")
      .option("hoodie.clustering.plan.strategy.target.file.max.bytes", "1073741824")
      .option("hoodie.clustering.plan.strategy.small.file.limit", "629145600")
      .option("hoodie.clustering.plan.strategy.max.bytes.per.group", "2147483648")
      .option(DataSourceWriteOptions.ENABLE_ROW_WRITER.key(), clusteringAsRow)
      .option(HoodieClusteringConfig.LAYOUT_OPTIMIZE_STRATEGY.key(), layoutOptimizationStrategy)
      .option(HoodieClusteringConfig.LAYOUT_OPTIMIZE_SPATIAL_CURVE_BUILD_METHOD.key(), curveCompositionStrategy)
      .option(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key, "begin_lat,begin_lon")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val hudiMetaClient = createMetaClient(basePath)

    val lastCommit = hudiMetaClient.getActiveTimeline.getAllCommitsTimeline.lastInstant().get()

    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, lastCommit.getAction)
    assertEquals(HoodieInstant.State.COMPLETED, lastCommit.getState)

    val readDf =
      spark.read
        .format("hudi")
        .load(basePath)

    val readDfSkip =
      spark.read
        .option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key(), "true")
        .options(metadataOpts)
        .format("hudi")
        .load(basePath)

    assertEquals(targetRecordsCount, readDf.count())
    assertEquals(targetRecordsCount, readDfSkip.count())

    readDf.createOrReplaceTempView("hudi_snapshot_raw")
    readDfSkip.createOrReplaceTempView("hudi_snapshot_skipping")

    def select(tableName: String) =
      spark.sql(s"SELECT * FROM $tableName WHERE begin_lat >= 0.49 AND begin_lat < 0.51 AND begin_lon >= 0.49 AND begin_lon < 0.51")

    assertRowsMatch(
      select("hudi_snapshot_raw"),
      select("hudi_snapshot_skipping")
    )
  }

  private def assertRowsMatch(one: DataFrame, other: DataFrame) = {
    val rows = one.count()
    assert(rows == other.count() && one.intersect(other).count() == rows)
  }
}

object TestLayoutOptimization {
  def testLayoutOptimizationParameters(): java.util.stream.Stream[Arguments] = {
    // TableType, enableClusteringAsRow, layoutOptimizationStrategy, spatialCurveCompositionStrategy
    java.util.stream.Stream.of(
      arguments("COPY_ON_WRITE", "true", "linear", null),
      arguments("COPY_ON_WRITE", "true", "z-order", "direct"),
      arguments("COPY_ON_WRITE", "true", "z-order", "sample"),
      arguments("COPY_ON_WRITE", "true", "hilbert", "direct"),
      arguments("COPY_ON_WRITE", "true", "hilbert", "sample"),
      arguments("COPY_ON_WRITE", "false", "linear", null),
      arguments("COPY_ON_WRITE", "false", "z-order", "direct"),
      arguments("COPY_ON_WRITE", "false", "z-order", "sample"),
      arguments("COPY_ON_WRITE", "false", "hilbert", "direct"),
      arguments("COPY_ON_WRITE", "false", "hilbert", "sample"),

      arguments("MERGE_ON_READ", "true", "linear", null),
      arguments("MERGE_ON_READ", "true", "z-order", "direct"),
      arguments("MERGE_ON_READ", "true", "z-order", "sample"),
      arguments("MERGE_ON_READ", "true", "hilbert", "direct"),
      arguments("MERGE_ON_READ", "true", "hilbert", "sample"),
      arguments("MERGE_ON_READ", "false", "linear", null),
      arguments("MERGE_ON_READ", "false", "z-order", "direct"),
      arguments("MERGE_ON_READ", "false", "z-order", "sample"),
      arguments("MERGE_ON_READ", "false", "hilbert", "direct"),
      arguments("MERGE_ON_READ", "false", "hilbert", "sample")
    )
  }
}
