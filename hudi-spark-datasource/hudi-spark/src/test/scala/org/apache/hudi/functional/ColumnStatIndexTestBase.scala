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

import org.apache.hudi.{AvroConversionUtils, ColumnStatsIndexSupport, DataSourceWriteOptions, HoodieSparkUtils, PartitionStatsIndexSupport}
import org.apache.hudi.ColumnStatsIndexSupport.composeIndexSchema
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.avro.model.DecimalWrapper
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.{HoodieBaseFile, HoodieFileGroup, HoodieLogFile, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion, TableSchemaResolver}
import org.apache.hudi.common.table.view.FileSystemViewManager
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.functional.ColumnStatIndexTestBase.{ColumnStatsTestCase, ColumnStatsTestParams}
import org.apache.hudi.metadata.HoodieTableMetadataUtil
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.{HoodieSparkClientTestBase, LogFileColStatsTestUtil}

import org.apache.avro.Schema
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.sql.functions.{lit, typedLit}
import org.apache.spark.sql.types._
import org.junit.jupiter.api._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.provider.Arguments

import java.math.{BigDecimal => JBigDecimal, BigInteger}
import java.nio.ByteBuffer
import java.sql.{Date, Timestamp}
import java.util
import java.util.List
import java.util.stream.Collectors

import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`map AsJavaMap`
import scala.collection.immutable.TreeSet
import scala.util.Random

class ColumnStatIndexTestBase extends HoodieSparkClientTestBase {
  var spark: SparkSession = _
  var dfList: Seq[DataFrame] = Seq()

  val sourceTableSchema =
    new StructType()
      .add("c1", IntegerType)
      .add("c2", StringType)
      .add("c3", DecimalType(9, 3))
      .add("c4", TimestampType)
      .add("c5", ShortType)
      .add("c6", DateType)
      .add("c7", StringType) // HUDI-8909. To support Byte w/ partition stats index.
      .add("c8", ByteType)

  @BeforeEach
  override def setUp() {
    initPath()
    initQueryIndexConf()
    initSparkContexts()
    initHoodieStorage()

    setTableName("hoodie_test")
    initMetaClient()

    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

  protected def doWriteAndValidateColumnStats(params: ColumnStatsTestParams, addNestedFiled : Boolean = false): Unit = {

    val sourceJSONTablePath = getClass.getClassLoader.getResource(params.dataSourcePath).toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val preInputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    val inputDF = if (addNestedFiled) {
      preInputDF.withColumn("c9",
        functions.struct("c2").withField("c9_1_car_brand", lit("abc_brand")))
      .withColumn("c10",
        functions.struct("c2").withField("c10_1_car_brand", lit("abc_brand"))
      .withField("c10_1", functions.struct("c2")
        .withField("c10_2_1_nested_lvl2_field1", lit("random_val1"))
        .withField("c10_2_1_nested_lvl2_field2", lit("random_val2"))))
    } else {
      preInputDF
    }

    val writeOptions: Map[String, String] = params.hudiOpts ++ params.metadataOpts

    inputDF
      .sort("c1")
      .repartition(params.numPartitions, new Column("c1"))
      .write
      .format("hudi")
      .options(writeOptions)
      .option(DataSourceWriteOptions.OPERATION.key, params.operation)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key(), String.valueOf(params.parquetMaxFileSize))
      .option(HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT.key(), String.valueOf(params.smallFileLimit))
      .option(HoodieWriteConfig.MARKERS_TIMELINE_SERVER_BASED_BATCH_INTERVAL_MS.key, "10")
      .mode(params.saveMode)
      .save(basePath)
    dfList = dfList :+ inputDF

    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()

    if (params.shouldValidateColStats) {
      // Currently, routine manually validating the column stats (by actually reading every column of every file)
      // only supports parquet files. Therefore we skip such validation when delta-log files are present, and only
      // validate in following cases: (1) COW: all operations; (2) MOR: insert only.
      val shouldValidateColumnStatsManually = (params.testCase.tableType == HoodieTableType.COPY_ON_WRITE ||
        params.operation.equals(DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)) && params.shouldValidateManually

      validateColumnStatsIndex(params.testCase, params.metadataOpts, params.expectedColStatsSourcePath,
        shouldValidateColumnStatsManually, params.validationSortColumns)
    } else if (params.shouldValidatePartitionStats) {
      validatePartitionStatsIndex(params.testCase, params.metadataOpts, params.expectedColStatsSourcePath)
    }
  }

  protected def buildPartitionStatsTableManually(tablePath: String,
                                                 includedCols: Seq[String],
                                                 indexedCols: Seq[String],
                                                 indexSchema: StructType): DataFrame = {
    val metaClient = HoodieTableMetaClient.builder().setConf(new HadoopStorageConfiguration(jsc.hadoopConfiguration())).setBasePath(tablePath).build()
    val fsv = FileSystemViewManager.createInMemoryFileSystemView(new HoodieSparkEngineContext(jsc), metaClient, HoodieMetadataConfig.newBuilder().enable(false).build())
    fsv.loadAllPartitions()
    val allPartitions = fsv.getPartitionNames.stream().map[String](partitionPath => partitionPath).collect(Collectors.toList[String]).asScala
    spark.createDataFrame(
      allPartitions.flatMap(partition => {
        val df = spark.read.format("hudi").load(tablePath) // assumes its partition table, but there is only one partition.
        val exprs: Seq[String] =
          s"${if (HoodieSparkUtils.gteqSpark4_0) typedLit("") else "'" + typedLit("") + "'"} AS file" +:
            s"sum(1) AS valueCount" +:
            df.columns
              .filter(col => includedCols.contains(col))
              .filter(col => indexedCols.contains(col))
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

  protected def buildColumnStatsTableManually(tablePath: String,
                                              includedCols: Seq[String],
                                            indexedCols: Seq[String],
                                            indexSchema: StructType,
                                            sourceTableSchema: StructType): DataFrame = {
    val metaClient = HoodieTableMetaClient.builder().setConf(new HadoopStorageConfiguration(jsc.hadoopConfiguration())).setBasePath(tablePath).build()
    val fsv = FileSystemViewManager.createInMemoryFileSystemView(new HoodieSparkEngineContext(jsc), metaClient, HoodieMetadataConfig.newBuilder().enable(false).build())
    fsv.loadAllPartitions()
    val filegroupList = fsv.getAllFileGroups.collect(Collectors.toList[HoodieFileGroup])
    val baseFilesList = filegroupList.stream().flatMap(fileGroup => fileGroup.getAllBaseFiles).collect(Collectors.toList[HoodieBaseFile])
    val baseFiles = baseFilesList.stream()
      .map[StoragePath](baseFile => baseFile.getStoragePath).collect(Collectors.toList[StoragePath]).asScala

    val baseFilesDf = spark.createDataFrame(
      baseFiles.flatMap(file => {
        val df = spark.read.schema(sourceTableSchema).parquet(file.toString)
        val exprs: Seq[String] =
          s"${if (HoodieSparkUtils.gteqSpark4_0) typedLit(file.getName) else "'" + typedLit(file.getName) + "'"} AS fileName" +:
            s"sum(1) AS valueCount" +:
            includedCols.union(indexedCols).distinct.sorted.flatMap(col => {
          val minColName = s"`${col}_minValue`"
          val maxColName = s"`${col}_maxValue`"
          if (indexedCols.contains(col)) {
            Seq(
              s"min($col) AS $minColName",
              s"max($col) AS $maxColName",
              s"sum(cast(isnull($col) AS long)) AS `${col}_nullCount`"
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

    if (metaClient.getTableConfig.getTableType == HoodieTableType.COPY_ON_WRITE) {
      baseFilesDf // COW table
    } else {
      val allLogFiles = filegroupList.stream().flatMap(fileGroup => fileGroup.getAllFileSlices)
        .flatMap(fileSlice => fileSlice.getLogFiles)
        .collect(Collectors.toList[HoodieLogFile])
      if (allLogFiles.isEmpty) {
        baseFilesDf // MOR table, but no log files.
      } else {
        val colsToGenerateStats = indexedCols // check for included cols
        val writerSchemaOpt = LogFileColStatsTestUtil.getSchemaForTable(metaClient)
        val latestCompletedCommit = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get().requestedTime
        baseFilesDf.union(getColStatsFromLogFiles(allLogFiles, latestCompletedCommit,
          scala.collection.JavaConverters.seqAsJavaList(colsToGenerateStats),
          metaClient,
          writerSchemaOpt: org.apache.hudi.common.util.Option[Schema],
          HoodieMetadataConfig.MAX_READER_BUFFER_SIZE_PROP.defaultValue(),
          indexSchema))
      }
    }
  }

  protected def getColStatsFromLogFiles(logFiles: List[HoodieLogFile], latestCommit: String, columnsToIndex: util.List[String],
                                        datasetMetaClient: HoodieTableMetaClient,
                                        writerSchemaOpt: org.apache.hudi.common.util.Option[Schema],
                                        maxBufferSize: Integer,
                                        indexSchema: StructType): DataFrame = {
    val colStatsEntries = logFiles.stream().map[org.apache.hudi.common.util.Option[Row]](logFile => {
      try {
        getColStatsFromLogFile(logFile.getPath.toString, latestCommit, columnsToIndex, datasetMetaClient, writerSchemaOpt, maxBufferSize)
      } catch {
        case e: Exception =>
          throw e
      }
    }).filter(rowOpt => rowOpt.isPresent).map[Row](rowOpt => rowOpt.get()).collect(Collectors.toList[Row])
    spark.createDataFrame(colStatsEntries, indexSchema)
  }

  protected def getColStatsFromLogFile(logFilePath: String,
                                       latestCommit: String,
                                       columnsToIndex: util.List[String],
                                       datasetMetaClient: HoodieTableMetaClient,
                                       writerSchemaOpt: org.apache.hudi.common.util.Option[Schema],
                                       maxBufferSize: Integer
                                      ): org.apache.hudi.common.util.Option[Row] = {
    LogFileColStatsTestUtil.getLogFileColumnRangeMetadata(logFilePath, datasetMetaClient, latestCommit,
      columnsToIndex, writerSchemaOpt, maxBufferSize)
  }

  protected def validateColumnsToIndex(metaClient: HoodieTableMetaClient, expectedColsToIndex: Seq[String]): Unit = {
    val indexDefn = metaClient.getIndexMetadata.get().getIndexDefinitions.get(PARTITION_NAME_COLUMN_STATS)
    assertEquals(expectedColsToIndex.sorted, indexDefn.getSourceFields.asScala.toSeq.sorted)
  }

  protected def validateNonExistantColumnsToIndexDefn(metaClient: HoodieTableMetaClient): Unit = {
    assertTrue(!metaClient.getIndexMetadata.get().getIndexDefinitions.containsKey(PARTITION_NAME_COLUMN_STATS))
  }

  protected def validateColumnStatsIndex(testCase: ColumnStatsTestCase,
                                         metadataOpts: Map[String, String],
                                         expectedColStatsSourcePath: String,
                                         validateColumnStatsManually: Boolean,
                                         validationSortColumns: Seq[String]): Unit = {
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()
    metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
    val schemaUtil = new TableSchemaResolver(metaClient)
    val tableSchema = schemaUtil.getTableAvroSchema(false)
    val localSourceTableSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableSchema)

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, localSourceTableSchema, metadataConfig, metaClient)
    val indexedColumnswithMeta: Set[String] = metaClient.getIndexMetadata.get().getIndexDefinitions.get(PARTITION_NAME_COLUMN_STATS).getSourceFields.asScala.toSet
    val indexedColumns = indexedColumnswithMeta.filter(colName => !HoodieTableMetadataUtil.META_COL_SET_TO_INDEX.contains(colName))
    val sortedIndexedColumns : Set[String] = TreeSet(indexedColumns.toSeq:_*)
    val (expectedColStatsSchema, _) = composeIndexSchema(sortedIndexedColumns.toSeq, indexedColumns.toSeq, localSourceTableSchema)

    columnStatsIndex.loadTransposed(indexedColumns.toSeq, testCase.shouldReadInMemory) { transposedColStatsDF =>
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

      if (validateColumnStatsManually) {
        // TODO(HUDI-4557): support validation of column stats of avro log files
        // Collect Column Stats manually (reading individual Parquet files)
        val manualColStatsTableDF =
        buildColumnStatsTableManually(basePath, indexedColumns.toSeq, indexedColumns.toSeq, expectedColStatsSchema, localSourceTableSchema)

        assertEquals(asJson(sort(manualColStatsTableDF, validationSortColumns)),
          asJson(sort(transposedColStatsDF, validationSortColumns)))
      }
    }
  }

  protected def validatePartitionStatsIndex(testCase: ColumnStatsTestCase,
                                            metadataOpts: Map[String, String],
                                            expectedColStatsSourcePath: String): Unit = {
    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()
    val schemaUtil = new TableSchemaResolver(metaClient)
    val tableSchema = schemaUtil.getTableAvroSchema(false)
    val localSourceTableSchema = AvroConversionUtils.convertAvroSchemaToStructType(tableSchema)

    val pStatsIndex = new PartitionStatsIndexSupport(spark, localSourceTableSchema, metadataConfig, metaClient)
    val indexedColumnswithMeta: Set[String] = metaClient.getIndexMetadata.get().getIndexDefinitions.get(PARTITION_NAME_COLUMN_STATS).getSourceFields.asScala.toSet
    val pIndexedColumns = indexedColumnswithMeta.filter(colName => !HoodieTableMetadataUtil.META_COL_SET_TO_INDEX.contains(colName))
      .toSeq.sorted

    val (pExpectedColStatsSchema, _) = composeIndexSchema(pIndexedColumns, pIndexedColumns, localSourceTableSchema)
    val pValidationSortColumns = if (pIndexedColumns.contains("c5")) {
      Seq("c1_maxValue", "c1_minValue", "c2_maxValue", "c2_minValue", "c3_maxValue",
        "c3_minValue", "c5_maxValue", "c5_minValue")
    } else {
      Seq("c1_maxValue", "c1_minValue", "c2_maxValue", "c2_minValue", "c3_maxValue", "c3_minValue")
    }

    pStatsIndex.loadTransposed(localSourceTableSchema.fieldNames, testCase.shouldReadInMemory) { pTransposedColStatsDF =>
      // Match against expected column stats table
      val pExpectedColStatsIndexTableDf = {
        spark.read
          .schema(pExpectedColStatsSchema)
          .json(getClass.getClassLoader.getResource(expectedColStatsSourcePath).toString)
      }

      val colsToDrop = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
        Seq("fileName")
      } else {
        Seq("fileName","valueCount") // for MOR, value count may not match, since w/ we could have repeated updates across multiple log files.
        // So, value count might be larger w/ MOR stats when compared to calculating it manually.
      }

      assertEquals(asJson(sort(pExpectedColStatsIndexTableDf.drop(colsToDrop: _*), pValidationSortColumns)),
        asJson(sort(pTransposedColStatsDF.drop(colsToDrop: _*), pValidationSortColumns)))

      val convertedSchema = AvroConversionUtils.convertAvroSchemaToStructType(AvroConversionUtils.convertStructTypeToAvroSchema(pExpectedColStatsSchema, "col_stats_schema"))

      if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
        val manualColStatsTableDF =
          buildPartitionStatsTableManually(basePath, pIndexedColumns, pIndexedColumns, convertedSchema)

        assertEquals(asJson(sort(manualColStatsTableDF.drop(colsToDrop: _*), pValidationSortColumns)),
          asJson(sort(pTransposedColStatsDF.drop(colsToDrop: _*), pValidationSortColumns)))
      }
    }
  }

  protected def generateRandomDataFrame(spark: SparkSession): DataFrame = {
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

  protected def asJson(df: DataFrame) =
    df.toJSON
      .select("value")
      .collect()
      .toSeq
      .map(_.getString(0))
      .mkString("\n")

  protected def sort(df: DataFrame): DataFrame = {
    sort(df, Seq("c1_maxValue", "c1_minValue"))
  }

  private def sort(df: DataFrame, sortColumns: Seq[String]): DataFrame = {
    //val sortedCols = df.columns.sorted
    // Sort dataset by specified columns (to minimize non-determinism in case multiple files have the same
    // value of the first column)
    df.select(sortColumns.head, sortColumns.tail: _*)
      .sort(sortColumns.head, sortColumns.tail: _*)
      .select(sortColumns.head, sortColumns.tail: _*)
  }
}

object ColumnStatIndexTestBase {

  case class ColumnStatsTestCase(tableType: HoodieTableType, shouldReadInMemory: Boolean, tableVersion: Int)

  // General providers (both in-memory and on-disk variants)
  def testMetadataColumnStatsIndexParams: java.util.stream.Stream[Arguments] = {
    testMetadataColumnStatsIndexParams(true)
  }

  def testMetadataPartitionStatsIndexParams: java.util.stream.Stream[Arguments] = {
    testMetadataColumnStatsIndexParams(false)
  }

  def testMetadataColumnStatsIndexParams(testV6: Boolean): java.util.stream.Stream[Arguments] = {
    val currentVersionCode = HoodieTableVersion.current().versionCode()
    java.util.stream.Stream.of(
      HoodieTableType.values().toStream.flatMap { tableType =>
        val v6Seq = if (testV6) {
          Seq(
            Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion = 6)),
            Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = false, tableVersion = 6))
          )
        } else {
          Seq.empty
        }

        v6Seq ++ Seq(
          Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion = 8)),
          Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = false, tableVersion = 8)),
          Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion = currentVersionCode)),
          Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = false, tableVersion = currentVersionCode))
        )
      }: _*
    )
  }

  // In-memory providers (only shouldReadInMemory = true)
  def testMetadataColumnStatsIndexParamsInMemory: java.util.stream.Stream[Arguments] = {
    testMetadataColumnStatsIndexParamsInMemory(true)
  }

  def testMetadataPartitionStatsIndexParamsInMemory: java.util.stream.Stream[Arguments] = {
    testMetadataColumnStatsIndexParamsInMemory(false)
  }

  def testMetadataColumnStatsIndexParamsInMemory(testV6: Boolean): java.util.stream.Stream[Arguments] = {
    val currentVersionCode = HoodieTableVersion.current().versionCode()
    java.util.stream.Stream.of(
      HoodieTableType.values().toStream.flatMap { tableType =>
        val v6Seq = if (testV6) {
          Seq(Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion = 6)))
        } else {
          Seq.empty
        }

        v6Seq ++ Seq(
          Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion = 8)),
          Arguments.arguments(ColumnStatsTestCase(tableType, shouldReadInMemory = true, tableVersion = currentVersionCode))
        )
      }: _*
    )
  }


  // MOR-only providers
  def testMetadataColumnStatsIndexParamsForMOR: java.util.stream.Stream[Arguments] = {
    testMetadataColumnStatsIndexParamsForMOR(true)
  }

  def testMetadataPartitionStatsIndexParamsForMOR: java.util.stream.Stream[Arguments] = {
    testMetadataColumnStatsIndexParamsForMOR(false)
  }

  def testMetadataColumnStatsIndexParamsForMOR(testV6: Boolean): java.util.stream.Stream[Arguments] = {
    val currentVersionCode = HoodieTableVersion.current().versionCode()
    java.util.stream.Stream.of(
      (if (testV6) Seq(
        Arguments.arguments(ColumnStatsTestCase(HoodieTableType.MERGE_ON_READ, shouldReadInMemory = true, tableVersion = 6)),
        Arguments.arguments(ColumnStatsTestCase(HoodieTableType.MERGE_ON_READ, shouldReadInMemory = false, tableVersion = 6))
      ) else Seq.empty) ++ Seq(
        Arguments.arguments(ColumnStatsTestCase(HoodieTableType.MERGE_ON_READ, shouldReadInMemory = true, tableVersion = 8)),
        Arguments.arguments(ColumnStatsTestCase(HoodieTableType.MERGE_ON_READ, shouldReadInMemory = false, tableVersion = 8)),
        Arguments.arguments(ColumnStatsTestCase(HoodieTableType.MERGE_ON_READ, shouldReadInMemory = true, tableVersion = currentVersionCode)),
        Arguments.arguments(ColumnStatsTestCase(HoodieTableType.MERGE_ON_READ, shouldReadInMemory = false, tableVersion = currentVersionCode))
      ): _*
    )
  }


  // TableType / partition column providers (tableVersion encoded as String like your original)
  def testTableTypePartitionTypeParams: java.util.stream.Stream[Arguments] = {
    testTableTypePartitionTypeParams(true)
  }

  def testTableTypePartitionTypeParamsNoV6: java.util.stream.Stream[Arguments] = {
    testTableTypePartitionTypeParams(false)
  }

  def testTableTypePartitionTypeParams(testV6: Boolean): java.util.stream.Stream[Arguments] = {
    val currentVersionCode = HoodieTableVersion.current().versionCode().toString
    val v6Seq = if (testV6) {
      Seq(
        Arguments.arguments(HoodieTableType.COPY_ON_WRITE, "c8", "6"),
        Arguments.arguments(HoodieTableType.COPY_ON_WRITE, "", "6"),
        Arguments.arguments(HoodieTableType.MERGE_ON_READ, "c8", "6"),
        Arguments.arguments(HoodieTableType.MERGE_ON_READ, "", "6")
      )
    } else {
      Seq.empty
    }

    java.util.stream.Stream.of(
      (v6Seq ++ Seq(
        // Table version 8
        Arguments.arguments(HoodieTableType.COPY_ON_WRITE, "c8", "8"),
        Arguments.arguments(HoodieTableType.COPY_ON_WRITE, "", "8"),
        Arguments.arguments(HoodieTableType.MERGE_ON_READ, "c8", "8"),
        Arguments.arguments(HoodieTableType.MERGE_ON_READ, "", "8"),

        // Table version current
        Arguments.arguments(HoodieTableType.COPY_ON_WRITE, "c8", currentVersionCode),
        Arguments.arguments(HoodieTableType.COPY_ON_WRITE, "", currentVersionCode),
        Arguments.arguments(HoodieTableType.MERGE_ON_READ, "c8", currentVersionCode),
        Arguments.arguments(HoodieTableType.MERGE_ON_READ, "", currentVersionCode)
      )): _*
    )
  }

  trait WrapperCreator {
    def create(orig: JBigDecimal, sch: Schema): DecimalWrapper
  }

  // Test cases for column stats index with DecimalWrapper
  def decimalWrapperTestCases: java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      // Test case 1: "ByteBuffer Test" – using an original JBigDecimal.
      Arguments.of(
        "ByteBuffer Test",
        new JBigDecimal("123.45"),
        new WrapperCreator {
          override def create(orig: JBigDecimal, sch: Schema): DecimalWrapper =
            new DecimalWrapper {
              // Return a ByteBuffer computed via Avro's DecimalConversion.`
              override def getValue: ByteBuffer =
                ColumnStatsIndexSupport.decConv.toBytes(orig, sch, sch.getLogicalType)
            }
        }
      ),
      // Test case 2: "Java BigDecimal Test" – again using a JBigDecimal.
      Arguments.of(
        "Java BigDecimal Test",
        new JBigDecimal("543.21"),
        new WrapperCreator {
          override def create(orig: JBigDecimal, sch: Schema): DecimalWrapper =
            new DecimalWrapper {
              override def getValue: ByteBuffer =
                ColumnStatsIndexSupport.decConv.toBytes(orig, sch, sch.getLogicalType)
            }
        }
      ),
      // Test case 3: "Scala BigDecimal Test" – using a Scala BigDecimal converted to JBigDecimal.
      Arguments.of(
        "Scala BigDecimal Test",
        scala.math.BigDecimal("987.65").bigDecimal,
        new WrapperCreator {
          override def create(orig: JBigDecimal, sch: Schema): DecimalWrapper =
            new DecimalWrapper {
              override def getValue: ByteBuffer =
                // Here we explicitly use orig (which comes from Scala BigDecimal converted to java.math.BigDecimal)
                ColumnStatsIndexSupport.decConv.toBytes(orig, sch, sch.getLogicalType)
            }
        }
      )
    )
  }

  case class ColumnStatsTestParams(testCase: ColumnStatsTestCase,
                                   metadataOpts: Map[String, String],
                                   hudiOpts: Map[String, String],
                                   dataSourcePath: String,
                                   expectedColStatsSourcePath: String,
                                   operation: String,
                                   saveMode: SaveMode,
                                   shouldValidateColStats: Boolean = true,
                                   shouldValidateManually: Boolean = true,
                                   latestCompletedCommit: String = null,
                                   numPartitions: Integer = 4,
                                   parquetMaxFileSize: Integer = 10 * 1024,
                                   smallFileLimit: Integer = 100 * 1024 * 1024,
                                   shouldValidatePartitionStats : Boolean = false,
                                   validationSortColumns : Seq[String] = Seq("c1_maxValue", "c1_minValue", "c2_maxValue",
                                     "c2_minValue", "c3_maxValue", "c3_minValue", "c5_maxValue", "c5_minValue"))
}
