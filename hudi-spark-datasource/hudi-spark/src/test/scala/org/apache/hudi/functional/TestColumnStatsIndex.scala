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

import org.apache.hudi.{AvroConversionUtils, ColumnStatsIndexSupport, DataSourceWriteOptions}
import org.apache.hudi.ColumnStatsIndexSupport.composeIndexSchema
import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.functional.ColumnStatIndexTestBase.ColumnStatsTestCase
import org.apache.hudi.storage.StoragePath
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, GreaterThan, Literal, Or}
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{CsvSource, EnumSource, MethodSource, ValueSource}

import scala.collection.JavaConverters._

@Tag("functional")
class TestColumnStatsIndex extends ColumnStatIndexTestBase {

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParams"))
  def testMetadataColumnStatsIndex(testCase: ColumnStatsTestCase): Unit = {
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
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
    ) ++ metadataOpts

    doWriteAndValidateColumnStats(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = "index/colstats/column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    doWriteAndValidateColumnStats(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/another-input-table-json",
      expectedColStatsSourcePath = "index/colstats/updated-column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)

    // NOTE: MOR and COW have different fixtures since MOR is bearing delta-log files (holding
    //       deferred updates), diverging from COW
    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-updated2-column-stats-index-table.json"
    } else {
      "index/colstats/mor-updated2-column-stats-index-table.json"
    }

    doWriteAndValidateColumnStats(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }


  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataColumnStatsIndexValueCount(tableType: HoodieTableType): Unit = {
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
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
    ) ++ metadataOpts

    val structSchema = StructType(StructField("c1", IntegerType, false) :: StructField("c2", StringType, true) :: Nil)
    val avroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(structSchema, "record", "")
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

    metaClient = HoodieTableMetaClient.reload(metaClient)

    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, structSchema, avroSchema, metadataConfig, metaClient)
    columnStatsIndex.loadTransposed(Seq("c2"), false) { transposedDF =>
      val result = transposedDF.select("valueCount", "c2_nullCount")
        .collect().head

      assertTrue(result.getLong(0) == 4)
      assertTrue(result.getLong(1) == 1)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testMetadataColumnStatsIndexPartialProjection(shouldReadInMemory: Boolean): Unit = {
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
      PRECOMBINE_FIELD.key -> "c1",
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

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, sourceTableAvroSchema, metadataConfig, metaClient)
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
  @CsvSource(value = Array("true", "false"))
  def testMetadataColumnStatsIndexPartialProjection(shouldReadInMemory: Boolean): Unit = {
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
      HoodieTableConfig.PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCommonConfig.RECONCILE_SCHEMA.key -> "true"
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

    metaClient = HoodieTableMetaClient.reload(metaClient)

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

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, sourceTableAvroSchema, metadataConfig, metaClient)

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { emptyTransposedColStatsDF =>
        assertEquals(0, emptyTransposedColStatsDF.collect().length)
      }
    }

    ////////////////////////////////////////////////////////////////////////
    // Case #2: Partial CSI projection
    //          Projection is requested for set of columns some of which are
    //          NOT indexed by the CSI
    ////////////////////////////////////////////////////////////////////////

    {
      // We have to include "c1", since we sort the expected outputs by this column
      val requestedColumns = Seq("c4", "c1")

      val (expectedColStatsSchema, _) = composeIndexSchema(requestedColumns.sorted, targetColumnsToIndex.toSet, sourceTableSchema)
      // Match against expected column stats table
      val expectedColStatsIndexTableDf =
        spark.read
          .schema(expectedColStatsSchema)
          .json(getClass.getClassLoader.getResource("index/colstats/partial-column-stats-index-table.json").toString)

      // Collect Column Stats manually (reading individual Parquet files)
      val manualColStatsTableDF =
        buildColumnStatsTableManually(basePath, requestedColumns, targetColumnsToIndex, expectedColStatsSchema)

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { partialTransposedColStatsDF =>
        assertEquals(expectedColStatsIndexTableDf.schema, partialTransposedColStatsDF.schema)
        // NOTE: We have to drop the `fileName` column as it contains semi-random components
        //       that we can't control in this test. Nevertheless, since we manually verify composition of the
        //       ColStats Index by reading Parquet footers from individual Parquet files, this is not an issue
        assertEquals(asJson(sort(expectedColStatsIndexTableDf)), asJson(sort(partialTransposedColStatsDF.drop("fileName"))))
        assertEquals(asJson(sort(manualColStatsTableDF)), asJson(sort(partialTransposedColStatsDF)))
      }
    }

    ////////////////////////////////////////////////////////////////////////
    // Case #3: Aligned CSI projection
    //          Projection is requested for set of columns some of which are
    //          indexed only for subset of files
    ////////////////////////////////////////////////////////////////////////

    {
      // NOTE: The update we're writing is intentionally omitting some of the columns
      //       present in an earlier source
      val missingCols = Seq("c2", "c3")
      val partialSourceTableSchema = StructType(sourceTableSchema.fields.filterNot(f => missingCols.contains(f.name)))

      val updateJSONTablePath = getClass.getClassLoader.getResource("index/colstats/partial-another-input-table-json").toString
      val updateDF = spark.read
        .schema(partialSourceTableSchema)
        .json(updateJSONTablePath)

      updateDF.repartition(4)
        .write
        .format("hudi")
        .options(opts)
        .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)

      metaClient = HoodieTableMetaClient.reload(metaClient)

      val requestedColumns = sourceTableSchema.fieldNames

      val (expectedColStatsSchema, _) = composeIndexSchema(requestedColumns.sorted, targetColumnsToIndex.toSet, sourceTableSchema)
      val expectedColStatsIndexUpdatedDF =
        spark.read
          .schema(expectedColStatsSchema)
          .json(getClass.getClassLoader.getResource("index/colstats/updated-partial-column-stats-index-table.json").toString)

      // Collect Column Stats manually (reading individual Parquet files)
      val manualUpdatedColStatsTableDF =
        buildColumnStatsTableManually(basePath, requestedColumns, targetColumnsToIndex, expectedColStatsSchema)

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, sourceTableAvroSchema, metadataConfig, metaClient)

      // Nevertheless, the last update was written with a new schema (that is a subset of the original table schema),
      // we should be able to read CSI, which will be properly padded (with nulls) after transposition
      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { transposedUpdatedColStatsDF =>
        assertEquals(expectedColStatsIndexUpdatedDF.schema, transposedUpdatedColStatsDF.schema)

        assertEquals(asJson(sort(expectedColStatsIndexUpdatedDF)), asJson(sort(transposedUpdatedColStatsDF.drop("fileName"))))
        assertEquals(asJson(sort(manualUpdatedColStatsTableDF)), asJson(sort(transposedUpdatedColStatsDF)))
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTranslateQueryFiltersIntoColumnStatsIndexFilterExpr(shouldReadInMemory: Boolean): Unit = {
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
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
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

    metaClient = HoodieTableMetaClient.reload(metaClient)

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
    val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, sourceTableAvroSchema, metadataConfig, metaClient)

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
        val andConditionActualFilter = andConditionFilters.map(translateIntoColumnStatsIndexFilterExpr(_, partialTransposedColStatsDF.schema))
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

      val expectedOrConditionIndexedFilter = Or(
        GreaterThan(UnresolvedAttribute("c1_maxValue"), Literal(1)),
        Literal(true)
      )

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { partialTransposedColStatsDF =>
        val orConditionActualFilter = orConditionFilters.map(translateIntoColumnStatsIndexFilterExpr(_, partialTransposedColStatsDF.schema))
          .reduce(And)
        assertEquals(expectedOrConditionIndexedFilter, orConditionActualFilter)
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

    val ranges = utils.readColumnStatsFromMetadata(storage, parquetFilePath, Seq("c1", "c2", "c3a", "c3b", "c3c", "c4", "c5", "c6", "c7", "c8").asJava)

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
}
