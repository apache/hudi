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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocatedFileStatus, Path}
import org.apache.hudi.ColumnStatsIndexSupport.composeIndexSchema
import org.apache.hudi.DataSourceWriteOptions.{PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.config.{HoodieStorageConfig, HoodieWriteConfig}
import org.apache.hudi.functional.TestColumnStatsIndex.ColumnStatsTestCase
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.{ColumnStatsIndexSupport, DataSourceWriteOptions}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource, ValueSource}

import java.math.BigInteger
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import scala.util.Random

@Tag("functional")
class TestColumnStatsIndex extends HoodieClientTestBase {
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

  @BeforeEach
  override def setUp() {
    initPath()
    initSparkContexts()
    initFileSystem()

    setTableName("hoodie_test")
    initMetaClient()

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

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
      // NOTE: Currently only this setting is used like following by different MT partitions:
      //          - Files: using it
      //          - Column Stats: NOT using it (defaults to doing "point-lookups")
      HoodieMetadataConfig.ENABLE_FULL_SCAN_LOG_FILES.key -> testCase.forceFullLogScan.toString,
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
    // Case #1: Empty CSI projection
    //          Projection is requested for columns which are NOT indexed
    //          by the CSI
    ////////////////////////////////////////////////////////////////////////

    {
      // These are NOT indexed
      val requestedColumns = Seq("c4")

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)

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

      val expectedColStatsSchema = composeIndexSchema(requestedColumns.sorted, sourceTableSchema)
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

      val expectedColStatsSchema = composeIndexSchema(requestedColumns.sorted, sourceTableSchema)
      val expectedColStatsIndexUpdatedDF =
        spark.read
          .schema(expectedColStatsSchema)
          .json(getClass.getClassLoader.getResource("index/colstats/updated-partial-column-stats-index-table.json").toString)

      // Collect Column Stats manually (reading individual Parquet files)
      val manualUpdatedColStatsTableDF =
        buildColumnStatsTableManually(basePath, requestedColumns, targetColumnsToIndex, expectedColStatsSchema)

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)

      // Nevertheless, the last update was written with a new schema (that is a subset of the original table schema),
      // we should be able to read CSI, which will be properly padded (with nulls) after transposition
      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { transposedUpdatedColStatsDF =>
        assertEquals(expectedColStatsIndexUpdatedDF.schema, transposedUpdatedColStatsDF.schema)

        assertEquals(asJson(sort(expectedColStatsIndexUpdatedDF)), asJson(sort(transposedUpdatedColStatsDF.drop("fileName"))))
        assertEquals(asJson(sort(manualUpdatedColStatsTableDF)), asJson(sort(transposedUpdatedColStatsDF)))
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

    val conf = new Configuration()
    val path = new Path(pathStr)
    val fs = path.getFileSystem(conf)

    val parquetFilePath = fs.listStatus(path).filter(fs => fs.getPath.getName.endsWith(".parquet")).toSeq.head.getPath

    val ranges = utils.readRangeFromParquetMetadata(conf, parquetFilePath,
      Seq("c1", "c2", "c3a", "c3b", "c3c", "c4", "c5", "c6", "c7", "c8").asJava)

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

  private def doWriteAndValidateColumnStats(testCase: ColumnStatsTestCase,
                                            metadataOpts: Map[String, String],
                                            hudiOpts: Map[String, String],
                                            dataSourcePath: String,
                                            expectedColStatsSourcePath: String,
                                            operation: String,
                                            saveMode: SaveMode): Unit = {
    val sourceJSONTablePath = getClass.getClassLoader.getResource(dataSourcePath).toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(hudiOpts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)

    // Only parquet files are supported for the validation against the generated column stats,
    // constructing the column stats  from parquet data files using Spark SQL and comparing that
    // with column stats index. This means that the following operations are support for such
    // validation: (1) COW: all operations; (2) MOR: insert only.
    val validateColumnStatsAgainstDataFiles =
    (testCase.tableType == HoodieTableType.COPY_ON_WRITE
      || operation.equals(DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL))
    validateColumnStatsIndex(
      testCase, metadataOpts, expectedColStatsSourcePath, validateColumnStatsAgainstDataFiles)
  }

  private def buildColumnStatsTableManually(tablePath: String,
                                            includedCols: Seq[String],
                                            indexedCols: Seq[String],
                                            indexSchema: StructType): DataFrame = {
    val files = {
      val it = fs.listFiles(new Path(tablePath), true)
      var seq = Seq[LocatedFileStatus]()
      while (it.hasNext) {
        seq = seq :+ it.next()
      }
      seq.filter(fs => fs.getPath.getName.endsWith(".parquet"))
    }

    spark.createDataFrame(
      files.flatMap(file => {
        val df = spark.read.schema(sourceTableSchema).parquet(file.getPath.toString)
        val exprs: Seq[String] =
          s"'${typedLit(file.getPath.getName)}' AS file" +:
          s"sum(1) AS valueCount" +:
            df.columns
              .filter(col => includedCols.contains(col))
              .flatMap(col => {
                val minColName = s"${col}_minValue"
                val maxColName = s"${col}_maxValue"
                if (indexedCols.contains(col)) {
                  Seq(
                    s"min($col) AS $minColName",
                    s"max($col) AS $maxColName",
                    s"sum(cast(isnull($col) AS long)) AS ${col}_nullCount"
                  )
                } else {
                  Seq(
                    s"null AS $minColName",
                    s"null AS $maxColName",
                    s"null AS ${col}_nullCount"
                  )
                }
              })

        df.selectExpr(exprs: _*)
          .collect()
      }).asJava,
      indexSchema
    )
  }

  private def validateColumnStatsIndex(testCase: ColumnStatsTestCase,
                                       metadataOpts: Map[String, String],
                                       expectedColStatsSourcePath: String,
                                       validateColumnStatsAgainstDataFiles: Boolean): Unit = {
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)

    val expectedColStatsSchema = composeIndexSchema(sourceTableSchema.fieldNames, sourceTableSchema)
    val validationSortColumns = Seq("c1_maxValue", "c1_minValue", "c2_maxValue", "c2_minValue")

    columnStatsIndex.loadTransposed(sourceTableSchema.fieldNames, testCase.shouldReadInMemory) { transposedColStatsDF =>
      // Match against expected column stats table
      val expectedColStatsIndexTableDf =
        spark.read
          .schema(expectedColStatsSchema)
          .json(getClass.getClassLoader.getResource(expectedColStatsSourcePath).toString)

      assertEquals(expectedColStatsIndexTableDf.schema, transposedColStatsDF.schema)
      // NOTE: We have to drop the `fileName` column as it contains semi-random components
      //       that we can't control in this test. Nevertheless, since we manually verify composition of the
      //       ColStats Index by reading Parquet footers from individual Parquet files, this is not an issue
      assertEquals(asJson(sort(expectedColStatsIndexTableDf, validationSortColumns)),
        asJson(sort(transposedColStatsDF.drop("fileName"), validationSortColumns)))

      if (validateColumnStatsAgainstDataFiles) {
        // TODO(HUDI-4557): support validation of column stats of avro log files
        // Collect Column Stats manually (reading individual Parquet files)
        val manualColStatsTableDF =
        buildColumnStatsTableManually(basePath, sourceTableSchema.fieldNames, sourceTableSchema.fieldNames, expectedColStatsSchema)

        assertEquals(asJson(sort(manualColStatsTableDF, validationSortColumns)),
          asJson(sort(transposedColStatsDF, validationSortColumns)))
      }
    }
  }

  private def generateRandomDataFrame(spark: SparkSession): DataFrame = {
    val sourceTableSchema =
      new StructType()
        .add("c1", IntegerType)
        .add("c2", StringType)
        // NOTE: We're testing different values for precision of the decimal to make sure
        //       we execute paths bearing different underlying representations in Parquet
        // REF: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#DECIMAL
        .add("c3a", DecimalType(9, 3))
        .add("c3b", DecimalType(10, 3))
        .add("c3c", DecimalType(20, 3))
        .add("c4", TimestampType)
        .add("c5", ShortType)
        .add("c6", DateType)
        .add("c7", BinaryType)
        .add("c8", ByteType)

    val rdd = spark.sparkContext.parallelize(0 to 1000, 1).map { item =>
      val c1 = Integer.valueOf(item)
      val c2 = Random.nextString(10)
      val c3a = java.math.BigDecimal.valueOf(Random.nextInt() % (1 << 24), 3)
      val c3b = java.math.BigDecimal.valueOf(Random.nextLong() % (1L << 32), 3)
      // NOTE: We cap it at 2^64 to make sure we're not exceeding target decimal's range
      val c3c = new java.math.BigDecimal(new BigInteger(64, new java.util.Random()), 3)
      val c4 = new Timestamp(System.currentTimeMillis())
      val c5 = java.lang.Short.valueOf(s"${(item + 16) / 10}")
      val c6 = Date.valueOf(s"${2020}-${item % 11 + 1}-${item % 28 + 1}")
      val c7 = Array(item).map(_.toByte)
      val c8 = java.lang.Byte.valueOf("9")

      RowFactory.create(c1, c2, c3a, c3b, c3c, c4, c5, c6, c7, c8)
    }

    spark.createDataFrame(rdd, sourceTableSchema)
  }

  private def asJson(df: DataFrame) =
    df.toJSON
      .select("value")
      .collect()
      .toSeq
      .map(_.getString(0))
      .mkString("\n")

  private def sort(df: DataFrame): DataFrame = {
    sort(df, Seq("c1_maxValue", "c1_minValue"))
  }

  private def sort(df: DataFrame, sortColumns: Seq[String]): DataFrame = {
    val sortedCols = df.columns.sorted
    // Sort dataset by specified columns (to minimize non-determinism in case multiple files have the same
    // value of the first column)
    df.select(sortedCols.head, sortedCols.tail: _*)
      .sort(sortColumns.head, sortColumns.tail: _*)
  }
}

object TestColumnStatsIndex {

  case class ColumnStatsTestCase(tableType: HoodieTableType, forceFullLogScan: Boolean, shouldReadInMemory: Boolean)

  def testMetadataColumnStatsIndexParams: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(HoodieTableType.values().toStream.flatMap(tableType =>
      Seq(Arguments.arguments(ColumnStatsTestCase(tableType, forceFullLogScan = false, shouldReadInMemory = true)),
        Arguments.arguments(ColumnStatsTestCase(tableType, forceFullLogScan = false, shouldReadInMemory = false)),
        Arguments.arguments(ColumnStatsTestCase(tableType, forceFullLogScan = true, shouldReadInMemory = false)),
        Arguments.arguments(ColumnStatsTestCase(tableType, forceFullLogScan = true, shouldReadInMemory = true)))
    ): _*)
  }
}
