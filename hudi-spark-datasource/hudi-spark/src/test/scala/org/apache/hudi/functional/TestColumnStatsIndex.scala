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

import org.apache.hudi.{AvroConversionUtils, ColumnStatsIndexSupport, DataSourceReadOptions, DataSourceWriteOptions, HoodieSchemaConversionUtils}
import org.apache.hudi.ColumnStatsIndexSupport.composeIndexSchema
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.avro.model.DecimalWrapper
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.timeline.versioning.v1.InstantFileNameGeneratorV1
import org.apache.hudi.common.table.view.FileSystemViewManager
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR
import org.apache.hudi.common.util.{ParquetUtils, StringUtils}
import org.apache.hudi.config.{HoodieCleanConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.functional.ColumnStatIndexTestBase.{ColumnStatsTestCase, ColumnStatsTestParams, WrapperCreator}
import org.apache.hudi.metadata.HoodieIndexVersion
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, GreaterThan, Literal, Or}
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr
import org.apache.spark.sql.types._
import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, CsvSource, MethodSource}

import java.math.{BigDecimal => JBigDecimal}
import java.nio.ByteBuffer
import java.util.Collections
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

@Tag("functional-b")
class TestColumnStatsIndex extends ColumnStatIndexTestBase {

  protected def withRDDPersistenceValidation(f: => Unit): Unit = {
    org.apache.hudi.testutils.SparkRDDValidationUtils.withRDDPersistenceValidation(spark, new org.apache.hudi.testutils.SparkRDDValidationUtils.ThrowingRunnable {
      override def run(): Unit = f
    })
  }

  val DEFAULT_COLUMNS_TO_INDEX = Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
    HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c7","c8")

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParams"))
  def testMetadataColumnStatsIndex(testCase: ColumnStatsTestCase): Unit = {
    withRDDPersistenceValidation {
      val metadataOpts = Map(
        HoodieMetadataConfig.ENABLE.key -> "true",
        HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
      )

      val commonOpts = Map(
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4",
        HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
        DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
        RECORDKEY_FIELD.key -> "c1",
        HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
        HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
        "hoodie.compact.inline.max.delta.commits" -> "10",
        HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> testCase.tableVersion.toString
      ) ++ metadataOpts

      // write empty first commit to validate edge cases
      sparkSession.emptyDataFrame
        .write
        .format("hudi")
        .options(commonOpts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Overwrite)
        .save(basePath)

      doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
        dataSourcePath = "index/colstats/input-table-json",
        expectedColStatsSourcePath = "index/colstats/column-stats-index-table.json",
        operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append))

      doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
        dataSourcePath = "index/colstats/another-input-table-json",
        expectedColStatsSourcePath = "index/colstats/updated-column-stats-index-table.json",
        operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append))

      // NOTE: MOR and COW have different fixtures since MOR is bearing delta-log files (holding
      //       deferred updates), diverging from COW
      var expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
        "index/colstats/cow-updated2-column-stats-index-table.json"
      } else {
        "index/colstats/mor-updated2-column-stats-index-table.json"
      }

      doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
        dataSourcePath = "index/colstats/update-input-table-json",
        expectedColStatsSourcePath = expectedColStatsSourcePath,
        operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append))

      validateColumnsToIndex(metaClient, DEFAULT_COLUMNS_TO_INDEX)

      // update list of columns to explicit list of cols.
      val metadataOpts1 = Map(
        HoodieMetadataConfig.ENABLE.key -> "true",
        HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
        HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c5,c6,c7,c8" // ignore c4
      )

      expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
        "index/colstats/cow-updated3-column-stats-index-table.json"
      } else {
        "index/colstats/mor-updated3-column-stats-index-table.json"
      }

      doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
        dataSourcePath = "index/colstats/update5-input-table-json",
        expectedColStatsSourcePath = expectedColStatsSourcePath,
        operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append))

      validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
        HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1", "c2", "c3", "c5", "c6", "c7", "c8"))

      // lets explicitly override again. ignore c6
      // update list of columns to explicit list of cols.
      val metadataOpts2 = Map(
        HoodieMetadataConfig.ENABLE.key -> "true",
        HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
        HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c5,c7,c8" // ignore c4,c6
      )

      expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
        "index/colstats/cow-updated4-column-stats-index-table.json"
      } else {
        "index/colstats/mor-updated4-column-stats-index-table.json"
      }

      doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts2, commonOpts,
        dataSourcePath = "index/colstats/update6-input-table-json",
        expectedColStatsSourcePath = expectedColStatsSourcePath,
        operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append))

      validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
        HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1", "c2", "c3", "c5", "c7", "c8"))

      // update list of columns to explicit list of cols.
      val metadataOpts3 = Map(
        HoodieMetadataConfig.ENABLE.key -> "true",
        HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false"
      )
      // disable col stats
      doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts3, commonOpts,
        dataSourcePath = "index/colstats/update6-input-table-json",
        expectedColStatsSourcePath = expectedColStatsSourcePath,
        operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
        saveMode = SaveMode.Append,
        shouldValidateColStats = false,
        shouldValidateManually = false))

      metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
      validateNonExistantColumnsToIndexDefn(metaClient)
    }
  }

  /**
   * Tests nested field support with col stats // testMetadataColumnStatsIndexParamsInMemory
   */
  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsInMemory"))
  def testMetadataColumnStatsIndexNestedFields(testCase : ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key() -> "c1,c2,c3,c4,c5,c6,c7,c8,c9.c9_1_car_brand,c10.c10_1.c10_2_1_nested_lvl2_field2"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> testCase.tableVersion.toString
    ) ++ metadataOpts

    var expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-table-nested-1.json"
    } else {
      "index/colstats/mor-table-nested-1.json"
    }

    // expectation is that, the nested field will also be indexed.
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validationSortColumns = Seq("c1_maxValue", "c1_minValue", "c2_maxValue",
        "c2_minValue", "c3_maxValue", "c3_minValue", "c5_maxValue", "c5_minValue", "`c9.c9_1_car_brand_maxValue`", "`c9.c9_1_car_brand_minValue`",
      "`c10.c10_1.c10_2_1_nested_lvl2_field2_maxValue`","`c10.c10_1.c10_2_1_nested_lvl2_field2_minValue`")),
      addNestedFiled = true)

      validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
        HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c7","c8", "c9.c9_1_car_brand","c10.c10_1.c10_2_1_nested_lvl2_field2"))

    expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-table-nested-2.json"
    } else {
      "index/colstats/mor-table-nested-2.json"
    }

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0,
      validationSortColumns = Seq("c1_maxValue", "c1_minValue", "c2_maxValue",
        "c2_minValue", "c3_maxValue", "c3_minValue", "c5_maxValue", "c5_minValue", "`c9.c9_1_car_brand_maxValue`", "`c9.c9_1_car_brand_minValue`",
        "`c10.c10_1.c10_2_1_nested_lvl2_field2_maxValue`","`c10.c10_1.c10_2_1_nested_lvl2_field2_minValue`")),
      addNestedFiled = true)
  }

  /**
   * Tests data skipping with nested MAP and ARRAY fields in column stats index.
   * This test verifies that queries can efficiently skip files based on nested field values
   * within MAP and ARRAY types using the new Parquet-style accessor patterns.
   */
  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsInMemory"))
  def testMetadataColumnStatsIndexNestedMapArrayDataSkipping(testCase: ColumnStatsTestCase): Unit = {
    // Define nested struct schema
    val nestedSchema = new StructType()
      .add("nested_int", IntegerType, false)
      .add("level", StringType, true)

    // Define full schema with MAP and ARRAY of nested structs
    val testSchema = new StructType()
      .add("record_key", StringType, false)
      .add("partition_col", IntegerType, false)
      .add("nullable_map_field", MapType(StringType, nestedSchema), true)
      .add("array_field", ArrayType(nestedSchema), false)

    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key ->
        "record_key,partition_col,nullable_map_field.key_value.value.nested_int,nullable_map_field.key_value.value.level,array_field.array.nested_int,array_field.array.level"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test_nested_skipping",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "record_key",
      HoodieTableConfig.ORDERING_FIELDS.key -> "record_key",
      PARTITIONPATH_FIELD.key -> "partition_col",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key -> "10240",
      HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key -> "0",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> testCase.tableVersion.toString
    ) ++ metadataOpts

    // Batch 1 - Low Range Values (should be skipped when querying for nested_int > 200)
    val batch1Data = Seq(
      Row("key_001", 1,
        Map("item1" -> Row(50, "low"), "item2" -> Row(75, "low")),
        Array(Row(60, "low"), Row(80, "low"))),
      Row("key_002", 1,
        Map("item1" -> Row(30, "low"), "item2" -> Row(90, "low")),
        Array(Row(40, "low"), Row(70, "low")))
    )

    // Batch 2 - High Range MAP and ARRAY (should be read when querying for nested_int > 200)
    val batch2Data = Seq(
      Row("key_003", 1,
        Map("item1" -> Row(250, "high"), "item2" -> Row(275, "high")),
        Array(Row(260, "high"), Row(280, "high"))),
      Row("key_004", 1,
        Map("item1" -> Row(230, "high"), "item2" -> Row(290, "high")),
        Array(Row(240, "high"), Row(270, "high")))
    )

    // Batch 3 - Mixed Range (low MAP, high ARRAY)
    val batch3Data = Seq(
      Row("key_005", 2,
        Map("item1" -> Row(50, "mixed"), "item2" -> Row(75, "mixed")),
        Array(Row(260, "high"), Row(280, "high"))),
      Row("key_006", 2,
        Map("item1" -> Row(30, "mixed"), "item2" -> Row(90, "mixed")),
        Array(Row(240, "high"), Row(270, "high")))
    )

    // Write Batch 1
    val df1 = spark.createDataFrame(spark.sparkContext.parallelize(batch1Data), testSchema)
    df1.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    // Write Batch 2
    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(batch2Data), testSchema)
    df2.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    // Write Batch 3
    val df3 = spark.createDataFrame(spark.sparkContext.parallelize(batch3Data), testSchema)
    df3.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()

    // Query options with data skipping enabled
    val queryOpts = Map(
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true"
    ) ++ metadataOpts

    // Query 1: Filter on MAP nested_int (high range)
    val resultDf1 = spark.read.format("hudi")
      .options(queryOpts)
      .load(basePath)
      .filter("EXISTS(map_values(nullable_map_field), v -> v.nested_int > 200)")

    // Expected: 2 records from batch2 (key_003, key_004)
    assertArrayEquals(
      Array[Object]("key_003", "key_004"),
      resultDf1.select("record_key").collect().map(_.getString(0)).sorted.asInstanceOf[Array[Object]]
    )

    // Query 2: Filter on ARRAY nested_int (high range)
    val resultDf2 = spark.read.format("hudi")
      .options(queryOpts)
      .load(basePath)
      .filter("EXISTS(array_field, elem -> elem.nested_int > 200)")

    // Expected: 4 records from batch2 and batch3 (key_003, key_004, key_005, key_006)
    assertArrayEquals(
      Array[Object]("key_003", "key_004", "key_005", "key_006"),
      resultDf2.select("record_key").collect().map(_.getString(0)).sorted.asInstanceOf[Array[Object]]
    )

    // Query 3: Filter on MAP level field (string)
    val resultDf3 = spark.read.format("hudi")
      .options(queryOpts)
      .load(basePath)
      .filter("EXISTS(map_values(nullable_map_field), v -> v.level = 'high')")

    // Expected: 2 records from batch2
    assertArrayEquals(
      Array[Object]("key_003", "key_004"),
      resultDf3.select("record_key").collect().map(_.getString(0)).sorted.asInstanceOf[Array[Object]]
    )

    // Query 4: Combined filter (both MAP and ARRAY conditions)
    val resultDf4 = spark.read.format("hudi")
      .options(queryOpts)
      .load(basePath)
      .filter("EXISTS(map_values(nullable_map_field), v -> v.nested_int > 200) " +
        "AND EXISTS(array_field, elem -> elem.nested_int > 200)")

    // Expected: 2 records from batch2 only (both MAP and ARRAY have high values)
    assertArrayEquals(
      Array[Object]("key_003", "key_004"),
      resultDf4.select("record_key").collect().map(_.getString(0)).sorted.asInstanceOf[Array[Object]]
    )

    // Validate column stats were created for nested fields
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    val hoodieSchema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(testSchema, "record", "")
    val columnStatsIndex = new ColumnStatsIndexSupport(
      spark,
      testSchema,
      hoodieSchema,
      metadataConfig,
      metaClient
    )

    val indexedColumns = Seq(
      "nullable_map_field.key_value.value.nested_int",
      "array_field.array.nested_int"
    )

    columnStatsIndex.loadTransposed(indexedColumns, testCase.shouldReadInMemory) { transposedDF =>
      // Verify we have stats for all 3 file groups (may have more files for MOR due to updates)
      val fileCount = transposedDF.select("fileName").distinct().count()
      assertTrue(fileCount >= 3, s"Expected at least 3 files with column stats, got $fileCount")

      // Verify min/max ranges for MAP field
      val mapStats = transposedDF.select(
        "`nullable_map_field.key_value.value.nested_int_minValue`",
        "`nullable_map_field.key_value.value.nested_int_maxValue`"
      ).collect().map(row => (row.getInt(0), row.getInt(1))).sorted

      // Expected stats: Batch1[30,90], Batch2[230,290], Batch3[30,90]
      // We should have exactly one file with [230,290] (high range) and at least two with [30,90] (low range)
      val mapHighRangeCount = mapStats.count(stat => stat._1 == 230 && stat._2 == 290)
      val mapLowRangeCount = mapStats.count(stat => stat._1 == 30 && stat._2 == 90)
      assertEquals(1, mapHighRangeCount, "Expected exactly 1 file with MAP range [230,290]")
      assertTrue(mapLowRangeCount >= 2, s"Expected at least 2 files with MAP range [30,90], got $mapLowRangeCount")

      // Verify min/max ranges for ARRAY field
      val arrayStats = transposedDF.select(
        "`array_field.array.nested_int_minValue`",
        "`array_field.array.nested_int_maxValue`"
      ).collect().map(row => (row.getInt(0), row.getInt(1))).sorted

      // Expected stats: Batch1[40,80], Batch2[260,280], Batch3[240,280]
      // We should have exactly one file with [40,80] (low range) and at least two with high ranges
      val arrayLowRangeCount = arrayStats.count(stat => stat._1 == 40 && stat._2 == 80)
      val arrayHighRangeCount = arrayStats.count(stat => stat._1 >= 240 && stat._2 == 280)
      assertEquals(1, arrayLowRangeCount, "Expected exactly 1 file with ARRAY range [40,80]")
      assertTrue(arrayHighRangeCount >= 2, s"Expected at least 2 files with ARRAY high ranges, got $arrayHighRangeCount")
    }
    // Validate that indexed columns are registered correctly
    validateColumnsToIndex(metaClient, Seq(
      HoodieRecord.COMMIT_TIME_METADATA_FIELD,
      HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD,
      "record_key",
      "partition_col",
      "nullable_map_field.key_value.value.nested_int",
      "nullable_map_field.key_value.value.level",
      "array_field.array.nested_int",
      "array_field.array.level"
    ))
  }

  @ParameterizedTest
  @MethodSource(Array("testTableTypePartitionTypeParams"))
  def testMetadataColumnStatsIndexInitializationWithUpserts(tableType: HoodieTableType, partitionCol : String, tableVersion: Int): Unit = {
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      PARTITIONPATH_FIELD.key -> partitionCol,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "5",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> testCase.tableVersion.toString
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // updates
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // delete a subset of recs. this will add a delete log block for MOR table.
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/delete-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    // NOTE: MOR and COW have different fixtures since MOR is bearing delta-log files (holding
    //       deferred updates), diverging from COW

    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-bootstrap1-column-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap1-column-stats-index-table.json"
    }

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    val latestCompletedCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().requestedTime

    // lets validate that we have log files generated in case of MOR table
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      val metaClient = HoodieTableMetaClient.builder().setConf(new HadoopStorageConfiguration(jsc.hadoopConfiguration())).setBasePath(basePath).build()
      val fsv = FileSystemViewManager.createInMemoryFileSystemView(new HoodieSparkEngineContext(jsc), metaClient, HoodieMetadataConfig.newBuilder().enable(false).build())
      fsv.loadAllPartitions()
      val baseStoragePath = new StoragePath(basePath)
      val allPartitionPaths = fsv.getPartitionPaths
      allPartitionPaths.forEach(partitionPath => {
        val pPath = FSUtils.getRelativePartitionPath(baseStoragePath, partitionPath)
        assertTrue(fsv.getLatestFileSlices(pPath).filter(fileSlice => fileSlice.hasLogFiles).count() > 0)
      })
      fsv.close()
    }

    // updates a subset which are not deleted and enable col stats and validate bootstrap
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update3-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      latestCompletedCommit = latestCompletedCommit,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // trigger one more upsert and compaction (w/ MOR table) and validate.
    val expectedColStatsSourcePath1 = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-bootstrap2-column-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap2-column-stats-index-table.json"
    }

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update4-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath1,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      latestCompletedCommit = latestCompletedCommit,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    validateColumnsToIndex(metaClient, DEFAULT_COLUMNS_TO_INDEX)
  }

  @ParameterizedTest
  @MethodSource(Array("testTableTypePartitionTypeParams"))
  def testMetadataColumnStatsIndexInitializationWithRollbacks(tableType: HoodieTableType, partitionCol : String, tableVersion: Int): Unit = {
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      PARTITIONPATH_FIELD.key() -> partitionCol,
      "hoodie.write.markers.type" -> "DIRECT",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> testCase.tableVersion.toString
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidateColStats = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // updates
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    simulateFailureForLatestCommit(tableType, partitionCol)

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    // NOTE: MOR and COW have different fixtures since MOR is bearing delta-log files (holding
    //       deferred updates), diverging from COW

    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-bootstrap-rollback1-column-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap-rollback1-column-stats-index-table.json"
    }

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    val latestCompletedCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().requestedTime

    // updates a subset which are not deleted and enable col stats and validate bootstrap
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update3-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      latestCompletedCommit = latestCompletedCommit,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    assertTrue(metaClient.getActiveTimeline.getRollbackTimeline.countInstants() > 0)

    validateColumnsToIndex(metaClient, DEFAULT_COLUMNS_TO_INDEX)
  }

  def simulateFailureForLatestCommit(tableType: HoodieTableType, partitionCol: String) : Unit = {
    // simulate failure for latest commit.
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    var baseFileName : String = null
    var logFileName : String = null
    val lastCompletedCommit = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      val dataFiles = if (StringUtils.isNullOrEmpty(partitionCol)) {
        metaClient.getStorage.listFiles(new StoragePath(metaClient.getBasePath, "/"))
      } else {
        metaClient.getStorage.listFiles(new StoragePath(metaClient.getBasePath, "9"))
      }
      val logFileFileStatus = dataFiles.stream().filter(fileStatus => fileStatus.getPath.getName.contains(".log")).findFirst().get()
      logFileName = logFileFileStatus.getPath.getName
    } else {
      val dataFiles = if (StringUtils.isNullOrEmpty(partitionCol)) {
        metaClient.getStorage.listFiles(new StoragePath(metaClient.getBasePath.toString))
      } else {
        metaClient.getStorage.listFiles(new StoragePath(metaClient.getBasePath,  "9"))
      }
      val baseFileFileStatus = dataFiles.stream().filter(fileStatus => fileStatus.getPath.getName.contains(lastCompletedCommit.requestedTime)
        && fileStatus.getPath.getName.contains("parquet")).findFirst().get()
      baseFileName = baseFileFileStatus.getPath.getName
    }

    if (metaClient.getTableConfig.getTableVersion.lesserThan(HoodieTableVersion.EIGHT)) {
      val latestCompletedFileName = new InstantFileNameGeneratorV1().getFileName(lastCompletedCommit)
      metaClient.getStorage.deleteFile(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/" + latestCompletedFileName))
    } else {
      val latestCompletedFileName = INSTANT_FILE_NAME_GENERATOR.getFileName(lastCompletedCommit)
      metaClient.getStorage.deleteFile(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/timeline/" + latestCompletedFileName))
    }

    // re-create marker for the deleted file.
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      if (StringUtils.isNullOrEmpty(partitionCol)) {
        metaClient.getStorage.create(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/.temp/" + lastCompletedCommit.requestedTime + "/" + logFileName + ".marker.APPEND"))
      } else {
        metaClient.getStorage.create(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/.temp/" + lastCompletedCommit.requestedTime + "/9/" + logFileName + ".marker.APPEND"))
      }
    } else {
      if (StringUtils.isNullOrEmpty(partitionCol)) {
        metaClient.getStorage.create(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/.temp/" + lastCompletedCommit.requestedTime + "/" + baseFileName + ".marker.MERGE"))
      } else {
        metaClient.getStorage.create(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/.temp/" + lastCompletedCommit.requestedTime + "/9/" + baseFileName + ".marker.MERGE"))
      }
    }
  }

  @ParameterizedTest
  @MethodSource(Array("testMORDeleteBlocksParams"))
  def testMORDeleteBlocks(tableVersion: Int): Unit = {
    val tableType: HoodieTableType = HoodieTableType.MERGE_ON_READ
    val partitionCol = "c8"
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      PARTITIONPATH_FIELD.key() -> partitionCol,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "5",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> testCase.tableVersion.toString
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // updates
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val expectedColStatsSourcePath = "index/colstats/mor-delete-block1-column-stats-index-table.json"

    // delete a subset of recs. this will add a delete log block for MOR table.
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/delete-input-table-json/",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))
  }

  @ParameterizedTest
  @CsvSource(value = Array("'',6", "'',8", "c8,6", "c8,8"))
  def testColStatsWithCleanCOW(partitionCol: String, tableVersion: Int): Unit = {
    val tableType: HoodieTableType = HoodieTableType.COPY_ON_WRITE
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      PARTITIONPATH_FIELD.key() -> partitionCol,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "1",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> testCase.tableVersion.toString
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidateColStats = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    // updates 1
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-clean1-column-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap-rollback1-column-stats-index-table.json"
    }

    // updates 2
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update3-input-table-json/",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))
  }

  @ParameterizedTest
  @CsvSource(value = Array("'',6", "'',8", "c8,6", "c8,8"))
  def testColStatsWithCleanMOR(partitionCol: String, tableVersion: Int): Unit = {
    val tableType: HoodieTableType = HoodieTableType.MERGE_ON_READ
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      PARTITIONPATH_FIELD.key() -> partitionCol,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "1",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "2",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> testCase.tableVersion.toString
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidateColStats = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    // updates 1
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-clean1-column-stats-index-table.json"
    } else {
      "index/colstats/mor-clean1-column-stats-index-table.json"
    }

    // updates 2
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update3-input-table-json/",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    assertTrue(metaClient.getActiveTimeline.getCleanerTimeline.countInstants() > 0)
  }

  @ParameterizedTest
  @CsvSource(value = Array("COPY_ON_WRITE,6", "MERGE_ON_READ,6", "COPY_ON_WRITE,8", "MERGE_ON_READ,8"))
  def testMetadataColumnStatsIndexValueCount(tableType: HoodieTableType, tableVersion: Int): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion.toString
    ) ++ metadataOpts

    val structSchema = StructType(StructField("c1", IntegerType, false) :: StructField("c2", StringType, true) :: Nil)
    val schema = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(structSchema, "record", "")
    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "v1"), Row(2, "v2"), Row(3, null), Row(4, "v4"))),
      structSchema)

    inputDF
      .sort("c1", "c2")
      .write
      .format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()

    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, structSchema, schema, metadataConfig, metaClient)
    columnStatsIndex.loadTransposed(Seq("c2"), false) { transposedDF =>
      val result = transposedDF.select("valueCount", "c2_nullCount")
        .collect().head

      assertTrue(result.getLong(0) == 4)
      assertTrue(result.getLong(1) == 1)
    }
  }

  @ParameterizedTest
  @CsvSource(value = Array("true,6", "false,6", "true,8", "false,8"))
  def testMetadataColumnStatsWithFilesFilter(shouldReadInMemory: Boolean, tableVersion: Int): Unit = {
    val targetColumnsToIndex = Seq("c1", "c2", "c3")

    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> targetColumnsToIndex.mkString(",")
    )

    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      PARTITIONPATH_FIELD.key() -> "c8",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCommonConfig.RECONCILE_SCHEMA.key -> "true",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion.toString
    ) ++ metadataOpts

    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json-partition-pruning").toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(opts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()

    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    val fsv = FileSystemViewManager.createInMemoryFileSystemView(new HoodieSparkEngineContext(jsc), metaClient,
      HoodieMetadataConfig.newBuilder().enable(false).build())
    fsv.loadAllPartitions()

    val partitionPaths = fsv.getPartitionPaths
    val partitionToBaseFiles : java.util.Map[String, java.util.List[StoragePath]] = new java.util.HashMap[String, java.util.List[StoragePath]]

    partitionPaths.forEach(partitionPath =>
      partitionToBaseFiles.put(partitionPath.getName, fsv.getLatestBaseFiles(partitionPath.getName)
        .map[StoragePath](baseFile => baseFile.getStoragePath).collect(Collectors.toList[StoragePath]))
    )
    fsv.close()

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, sourceTableHoodieSchema, metadataConfig, metaClient)
    val requestedColumns = Seq("c1")
    // get all file names
    val stringEncoder: Encoder[String] = org.apache.spark.sql.Encoders.STRING
    val fileNameSet: Set[String] = columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { df =>
      val fileNames: Array[String] = df.select("fileName").as[String](stringEncoder).collect()
      val newFileNameSet: Set[String] = fileNames.toSet
      newFileNameSet
    }
    val targetFileName = fileNameSet.take(2)
    columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory, None, Some(targetFileName)) { df =>
      assertEquals(2, df.collect().length)
      val targetDFFileNameSet: Set[String] = df.select("fileName").as[String](stringEncoder).collect().toSet
      assertEquals(targetFileName, targetDFFileNameSet)
    }

    // fetch stats only for a subset of partitions
    // lets send in all file names, but only 1 partition to test that the prefix based lookup is effective.
    val expectedFileNames = partitionToBaseFiles.get("10").stream().map[String](baseFile => baseFile.getName).collect(Collectors.toSet[String])
    // even though fileNameSet (last arg) contains files from all partitions, since list of partitions passed in is just 1 (i.e 10), we should get matched only w/ files from
    // partition 10.
    columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory, Some(Set("10")), Some(fileNameSet)) { df =>
      assertEquals(expectedFileNames.size(), df.collect().length)
      val targetDFFileNameSet = df.select("fileName").as[String](stringEncoder).collectAsList().stream().collect(Collectors.toSet[String])
      assertEquals(expectedFileNames, targetDFFileNameSet)
    }

    // lets redo for 2 partitions.
    val expectedFileNames1 = partitionToBaseFiles.get("9").stream().map[String](baseFile => baseFile.getName).collect(Collectors.toList[String])
    expectedFileNames1.addAll(partitionToBaseFiles.get("11").stream().map[String](baseFile => baseFile.getName).collect(Collectors.toList[String]))
    Collections.sort(expectedFileNames1)
    columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory, Some(Set("9","11")), Some(fileNameSet)) { df =>
      assertEquals(expectedFileNames1.size, df.collect().length)
      val targetDFFileNameSet = df.select("fileName").as[String](stringEncoder).collectAsList().stream().collect(Collectors.toList[String])
      Collections.sort(targetDFFileNameSet)
      assertEquals(expectedFileNames1, targetDFFileNameSet)
    }
  }

  @ParameterizedTest
  @CsvSource(value = Array("true,6", "false,6", "true,8", "false,8"))
  def testMetadataColumnStatsIndexPartialProjection(shouldReadInMemory: Boolean, tableVersion: Int): Unit = {
    var targetColumnsToIndex = Seq("c1", "c2", "c3")

    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> targetColumnsToIndex.mkString(",")
    )

    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCommonConfig.RECONCILE_SCHEMA.key -> "true",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion.toString
    ) ++ metadataOpts

    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json").toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(opts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()

    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    ////////////////////////////////////////////////////////////////////////
    // Case #1: Empty CSI projection
    //          Projection is requested for columns which are NOT indexed
    //          by the CSI
    ////////////////////////////////////////////////////////////////////////

    {
      // These are NOT indexed
      val requestedColumns = Seq("c4")

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, sourceTableHoodieSchema, metadataConfig, metaClient)

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { emptyTransposedColStatsDF =>
        assertEquals(0, emptyTransposedColStatsDF.collect().length)
      }
    }

    ////////////////////////////////////////////////////////////////////////
    //          Projection is requested for set of columns some of which are
    //          indexed only for subset of files
    //   In commit1, we indexed c1,c2 and c3. in 2nd commit, we are indexing c5 in addition, but we update only a subset of records.
    //   So, we expect null stats for c5 against file groups which was not
    ////////////////////////////////////////////////////////////////////////

    {
      targetColumnsToIndex = Seq("c1", "c2", "c3","c5")
      val partialSourceTableSchema = StructType(sourceTableSchema.fields.filter(f => targetColumnsToIndex.contains(f.name)))

      val updateJSONTablePath = getClass.getClassLoader.getResource("index/colstats/evolved-cols-input-table-json").toString
      val updateDF = spark.read
        .schema(partialSourceTableSchema)
        .json(updateJSONTablePath)

      updateDF.repartition(4)
        .write
        .format("hudi")
        .options(opts)
        .option(HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key, "c1,c2,c3,c5")
        .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)

      metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()

      val requestedColumns = metaClient.getIndexMetadata.get().getIndexDefinitions.get(PARTITION_NAME_COLUMN_STATS)
        .getSourceFields.toSeq.filterNot(colName => colName.startsWith("_hoodie")).sorted.toSeq

      val (expectedColStatsSchema, _) = composeIndexSchema(requestedColumns, targetColumnsToIndex.toSeq, sourceTableSchema)
      val expectedColStatsIndexUpdatedDF =
        spark.read
          .schema(expectedColStatsSchema)
          .json(getClass.getClassLoader.getResource("index/colstats/updated-partial-column-stats-index-table.json").toString)

      // Collect Column Stats manually (reading individual Parquet files)
      val manualUpdatedColStatsTableDF =
        buildColumnStatsTableManually(basePath, requestedColumns, targetColumnsToIndex, expectedColStatsSchema, sourceTableSchema)

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, sourceTableHoodieSchema, metadataConfig, metaClient)

      // Nevertheless, the last update was written with a new schema (that is a subset of the original table schema),
      // we should be able to read CSI, which will be properly padded (with nulls) after transposition
      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { transposedUpdatedColStatsDF =>
        assertEquals(expectedColStatsIndexUpdatedDF.schema, transposedUpdatedColStatsDF.schema)

        //assertEquals(asJson(sort(expectedColStatsIndexUpdatedDF.drop("fileName"))), asJson(sort(transposedUpdatedColStatsDF.drop("fileName"))))
        assertEquals(asJson(sort(manualUpdatedColStatsTableDF)), asJson(sort(transposedUpdatedColStatsDF)))
      }
    }
  }

  @ParameterizedTest
  @CsvSource(value = Array("true,6", "false,6", "true,8", "false,8"))
  def testTranslateQueryFiltersIntoColumnStatsIndexFilterExpr(shouldReadInMemory: Boolean, tableVersion: Int): Unit = {
    val targetColumnsToIndex = Seq("c1", "c2", "c3")

    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> targetColumnsToIndex.mkString(",")
    )

    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      RECORDKEY_FIELD.key -> "c1",
      HoodieTableConfig.ORDERING_FIELDS.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> tableVersion.toString
    ) ++ metadataOpts

    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json").toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(opts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()

    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    ////////////////////////////////////////////////////////////////////////
    // NOTE: Partial CSI projection
    //          Projection is requested for set of columns some of which are
    //          NOT indexed by the CSI
    ////////////////////////////////////////////////////////////////////////

    // We have to include "c1", since we sort the expected outputs by this column
    val requestedColumns = Seq("c4", "c1")
    val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, sourceTableHoodieSchema, metadataConfig, metaClient)

    ////////////////////////////////////////////////////////////////////////
    // Query filter #1: c1 > 1 and c4 > 'c4 filed value'
    //                  We should filter only for c1
    ////////////////////////////////////////////////////////////////////////
    {
      val andConditionFilters = Seq(
        GreaterThan(AttributeReference("c1", IntegerType, nullable = true)(), Literal(1)),
        GreaterThan(AttributeReference("c4", StringType, nullable = true)(), Literal("c4 filed value"))
      )

      val expectedAndConditionIndexedFilter = And(
        GreaterThan(UnresolvedAttribute("c1_maxValue"), Literal(1)),
        Literal(true)
      )

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { partialTransposedColStatsDF =>
        val andConditionActualFilter = andConditionFilters.map(translateIntoColumnStatsIndexFilterExpr(_,
          indexedCols = targetColumnsToIndex))
          .reduce(And)
        assertEquals(expectedAndConditionIndexedFilter, andConditionActualFilter)
      }
    }

    ////////////////////////////////////////////////////////////////////////
    // Query filter #2: c1 > 1 or c4 > 'c4 filed value'
    //                  Since c4 is not indexed, we cannot filter any data
    ////////////////////////////////////////////////////////////////////////
    {
      val orConditionFilters = Seq(
        Or(GreaterThan(AttributeReference("c1", IntegerType, nullable = true)(), Literal(1)),
          GreaterThan(AttributeReference("c4", StringType, nullable = true)(), Literal("c4 filed value")))
      )

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { partialTransposedColStatsDF =>
        val orConditionActualFilter = orConditionFilters.map(translateIntoColumnStatsIndexFilterExpr(_,
          indexedCols = targetColumnsToIndex))
          .reduce(And)
        assertEquals(Literal("true").toString(), orConditionActualFilter.toString())
      }
    }
  }

  @Test
  def testParquetMetadataRangeExtraction(): Unit = {
    val df = generateRandomDataFrame(spark)

    val pathStr = tempDir.resolve("min-max").toAbsolutePath.toString

    df.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(pathStr)

    val utils = new ParquetUtils

    val path = new Path(pathStr)
    val storage = HoodieTestUtils.getStorage(new StoragePath(pathStr))
    val fs = path.getFileSystem(storage.getConf.unwrapAs(classOf[Configuration]))

    val parquetFilePath = new StoragePath(
      fs.listStatus(path).filter(fs => fs.getPath.getName.endsWith(".parquet")).toSeq.head.getPath.toUri)

    val ranges = utils.readColumnStatsFromMetadata(storage, parquetFilePath, Seq("c1", "c2", "c3a", "c3b", "c3c", "c4", "c5", "c6", "c7", "c8").asJava, HoodieIndexVersion.V1)

    ranges.asScala.foreach(r => {
      // NOTE: Unfortunately Parquet can't compute statistics for Timestamp column, hence we
      //       skip it in our assertions
      if (r.getColumnName.equals("c4")) {
        // scalastyle:off return
        return
        // scalastyle:on return
      }

      val min = r.getMinValue
      val max = r.getMaxValue

      assertNotNull(min)
      assertNotNull(max)
      assertTrue(r.getMinValue.asInstanceOf[Comparable[Object]].compareTo(r.getMaxValue.asInstanceOf[Object]) <= 0)
    })
  }

  @ParameterizedTest
  @MethodSource(Array("decimalWrapperTestCases"))
  def testDeserialize(description: String, expected: JBigDecimal, wrapperCreator: WrapperCreator): Unit = {
    val dt = DecimalType(10, 2)
    // Get the schema from the DecimalWrapper's Avro definition.
    val schema: Schema = DecimalWrapper.SCHEMA$.getField("value").schema()
    val wrapper = wrapperCreator.create(expected, schema)
    // Extract the underlying value.
    val unwrapped = ColumnStatsIndexSupport.tryUnpackValueWrapper(wrapper)
    // Optionally, for the "ByteBuffer Test" case, verify that the unwrapped value is a ByteBuffer.
    if (description.contains("ByteBuffer Test")) {
      assertTrue(unwrapped.isInstanceOf[ByteBuffer], "Expected a ByteBuffer")
    }
    // Deserialize into a java.math.BigDecimal.
    val deserialized = ColumnStatsIndexSupport.deserialize(unwrapped, dt)
    assertTrue(deserialized.isInstanceOf[JBigDecimal], "Deserialized value should be a java.math.BigDecimal")
    assertEquals(expected, deserialized.asInstanceOf[JBigDecimal],
      s"Decimal value from $description does not match")
  }
}

object TestColumnStatsIndex {
  def testMORDeleteBlocksParams: java.util.stream.Stream[Arguments] = {
    val currentVersionCode = HoodieTableVersion.current().versionCode().toString
    java.util.stream.Stream.of(Seq(
      Arguments.arguments("6"),
      Arguments.arguments("8"),
      Arguments.arguments(currentVersionCode)
    )
      : _*)
  }
}
