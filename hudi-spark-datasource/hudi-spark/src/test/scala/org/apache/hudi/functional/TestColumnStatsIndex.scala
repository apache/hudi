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
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.index.columnstats.ColumnStatsIndexHelper
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

import java.math.BigInteger
import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import scala.util.Random

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
    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown() = {
    cleanupFileSystem()
    cleanupSparkContexts()
  }

  @Test
  def testZIndexTableComposition(): Unit = {
    val targetParquetTablePath = tempDir.resolve("index/zorder/input-table").toAbsolutePath.toString
    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/zorder/input-table-json").toString

    bootstrapParquetInputTableFromJSON(sourceJSONTablePath, targetParquetTablePath)

    val inputDf =
    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
      spark.read
        .schema(sourceTableSchema)
        .parquet(targetParquetTablePath)

    val zorderedCols = Seq("c1", "c2", "c3", "c5", "c6", "c7", "c8")
    val zorderedColsSchemaFields = inputDf.schema.fields.filter(f => zorderedCols.contains(f.name)).toSeq

    // {@link TimestampType} is not supported, and will throw -- hence skipping "c4"
    val newZIndexTableDf =
      ColumnStatsIndexHelper.buildColumnStatsTableFor(
        inputDf.sparkSession,
        inputDf.inputFiles.toSeq.asJava,
        zorderedColsSchemaFields.asJava
      )

    val indexSchema =
      ColumnStatsIndexHelper.composeIndexSchema(
        sourceTableSchema.fields.filter(f => zorderedCols.contains(f.name)).toSeq.asJava
      )

    // Collect Z-index stats manually (reading individual Parquet files)
    val manualZIndexTableDf =
      buildColumnStatsTableManually(targetParquetTablePath, zorderedCols, indexSchema)

    // NOTE: Z-index is built against stats collected w/in Parquet footers, which will be
    //       represented w/ corresponding Parquet schema (INT, INT64, INT96, etc).
    //
    //       When stats are collected manually, produced Z-index table is inherently coerced into the
    //       schema of the original source Parquet base-file and therefore we have to similarly coerce newly
    //       built Z-index table (built off Parquet footers) into the canonical index schema (built off the
    //       original source file schema)
    assertEquals(asJson(sort(manualZIndexTableDf)), asJson(sort(newZIndexTableDf)))

    // Match against expected Z-index table
    val expectedZIndexTableDf =
      spark.read
        .schema(indexSchema)
        .json(getClass.getClassLoader.getResource("index/zorder/z-index-table.json").toString)

    assertEquals(asJson(sort(expectedZIndexTableDf)), asJson(sort(replace(newZIndexTableDf))))
  }

  @Test
  def testZIndexTableMerge(): Unit = {
    val testZIndexPath = new Path(basePath, "zindex")

    val firstParquetTablePath = tempDir.resolve("index/zorder/input-table").toAbsolutePath.toString
    val firstJSONTablePath = getClass.getClassLoader.getResource("index/zorder/input-table-json").toString

    // Bootstrap FIRST source Parquet table
    bootstrapParquetInputTableFromJSON(firstJSONTablePath, firstParquetTablePath)

    val zorderedCols = Seq("c1", "c2", "c3", "c5", "c6", "c7", "c8")
    val indexSchema =
      ColumnStatsIndexHelper.composeIndexSchema(
        sourceTableSchema.fields.filter(f => zorderedCols.contains(f.name)).toSeq.asJava
      )

    //
    // Bootstrap Z-index table
    //

    val firstCommitInstance = "0"
    val firstInputDf = spark.read.parquet(firstParquetTablePath)

    ColumnStatsIndexHelper.updateColumnStatsIndexFor(
      firstInputDf.sparkSession,
      sourceTableSchema,
      firstInputDf.inputFiles.toSeq.asJava,
      zorderedCols.asJava,
      testZIndexPath.toString,
      firstCommitInstance,
      Seq().asJava
    )

    // NOTE: We don't need to provide schema upon reading from Parquet, since Spark will be able
    //       to reliably retrieve it
    val initialZIndexTable =
    spark.read
      .parquet(new Path(testZIndexPath, firstCommitInstance).toString)

    val expectedInitialZIndexTableDf =
      spark.read
        .schema(indexSchema)
        .json(getClass.getClassLoader.getResource("index/zorder/z-index-table.json").toString)

    assertEquals(asJson(sort(expectedInitialZIndexTableDf)), asJson(sort(replace(initialZIndexTable))))

    // Bootstrap SECOND source Parquet table
    val secondParquetTablePath = tempDir.resolve("index/zorder/another-input-table").toAbsolutePath.toString
    val secondJSONTablePath = getClass.getClassLoader.getResource("index/zorder/another-input-table-json").toString

    bootstrapParquetInputTableFromJSON(secondJSONTablePath, secondParquetTablePath)

    val secondCommitInstance = "1"
    val secondInputDf =
      spark.read
        .schema(sourceTableSchema)
        .parquet(secondParquetTablePath)

    //
    // Update Z-index table
    //

    ColumnStatsIndexHelper.updateColumnStatsIndexFor(
      secondInputDf.sparkSession,
      sourceTableSchema,
      secondInputDf.inputFiles.toSeq.asJava,
      zorderedCols.asJava,
      testZIndexPath.toString,
      secondCommitInstance,
      Seq(firstCommitInstance).asJava
    )

    // NOTE: We don't need to provide schema upon reading from Parquet, since Spark will be able
    //       to reliably retrieve it
    val mergedZIndexTable =
    spark.read
      .parquet(new Path(testZIndexPath, secondCommitInstance).toString)

    val expectedMergedZIndexTableDf =
      spark.read
        .schema(indexSchema)
        .json(getClass.getClassLoader.getResource("index/zorder/z-index-table-merged.json").toString)

    assertEquals(asJson(sort(expectedMergedZIndexTableDf)), asJson(sort(replace(mergedZIndexTable))))
  }

  @Test
  def testColumnStatsTablesGarbageCollection(): Unit = {
    val targetParquetTablePath = tempDir.resolve("index/zorder/input-table").toAbsolutePath.toString
    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/zorder/input-table-json").toString

    bootstrapParquetInputTableFromJSON(sourceJSONTablePath, targetParquetTablePath)

    val inputDf = spark.read.parquet(targetParquetTablePath)

    val testColumnStatsIndexPath = new Path(tempDir.resolve("zindex").toAbsolutePath.toString)
    val fs = testColumnStatsIndexPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    // Try to save statistics
    ColumnStatsIndexHelper.updateColumnStatsIndexFor(
      inputDf.sparkSession,
      sourceTableSchema,
      inputDf.inputFiles.toSeq.asJava,
      Seq("c1","c2","c3","c5","c6","c7","c8").asJava,
      testColumnStatsIndexPath.toString,
      "2",
      Seq("0", "1").asJava
    )

    // Save again
    ColumnStatsIndexHelper.updateColumnStatsIndexFor(
      inputDf.sparkSession,
      sourceTableSchema,
      inputDf.inputFiles.toSeq.asJava,
      Seq("c1","c2","c3","c5","c6","c7","c8").asJava,
      testColumnStatsIndexPath.toString,
      "3",
      Seq("0", "1", "2").asJava
    )

    // Test old index table being cleaned up
    ColumnStatsIndexHelper.updateColumnStatsIndexFor(
      inputDf.sparkSession,
      sourceTableSchema,
      inputDf.inputFiles.toSeq.asJava,
      Seq("c1","c2","c3","c5","c6","c7","c8").asJava,
      testColumnStatsIndexPath.toString,
      "4",
      Seq("0", "1", "3").asJava
    )

    assertEquals(!fs.exists(new Path(testColumnStatsIndexPath, "2")), true)
    assertEquals(!fs.exists(new Path(testColumnStatsIndexPath, "3")), true)
    assertEquals(fs.exists(new Path(testColumnStatsIndexPath, "4")), true)
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

  private def buildColumnStatsTableManually(tablePath: String, zorderedCols: Seq[String], indexSchema: StructType) = {
    val files = {
      val it = fs.listFiles(new Path(tablePath), true)
      var seq = Seq[LocatedFileStatus]()
      while (it.hasNext) {
        seq = seq :+ it.next()
      }
      seq
    }

    spark.createDataFrame(
      files.flatMap(file => {
        val df = spark.read.schema(sourceTableSchema).parquet(file.getPath.toString)
        val exprs: Seq[String] =
          s"'${typedLit(file.getPath.getName)}' AS file" +:
            df.columns
              .filter(col => zorderedCols.contains(col))
              .flatMap(col => {
                val minColName = s"${col}_minValue"
                val maxColName = s"${col}_maxValue"
                Seq(
                  s"min($col) AS $minColName",
                  s"max($col) AS $maxColName",
                  s"sum(cast(isnull($col) AS long)) AS ${col}_num_nulls"
                )
              })

        df.selectExpr(exprs: _*)
          .collect()
      }).asJava,
      indexSchema
    )
  }

  def bootstrapParquetInputTableFromJSON(sourceJSONTablePath: String, targetParquetTablePath: String): Unit = {
    val jsonInputDF =
    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
      spark.read
        .schema(sourceTableSchema)
        .json(sourceJSONTablePath)

    jsonInputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("parquet")
      .mode("overwrite")
      .save(targetParquetTablePath)

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    // Have to cleanup additional artefacts of Spark write
    fs.delete(new Path(targetParquetTablePath, "_SUCCESS"), false)
  }

  def replace(ds: Dataset[Row]): DataFrame = {
    val uuidRegexp = "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}"

    val uuids =
      ds.selectExpr(s"regexp_extract(file, '(${uuidRegexp})')")
        .distinct()
        .collect()
        .map(_.getString(0))

    val uuidToIdx: UserDefinedFunction = functions.udf((fileName: String) => {
      val uuid = uuids.find(uuid => fileName.contains(uuid)).get
      fileName.replace(uuid, "xxx")
    })

    ds.withColumn("file", uuidToIdx(ds("file")))
  }

  private def generateRandomDataFrame(spark: SparkSession): DataFrame = {
    val sourceTableSchema =
      new StructType()
        .add("c1", IntegerType)
        .add("c2", StringType)
        // NOTE: We're testing different values for precision of the decimal to make sure
        //       we execute paths bearing different underlying representations in Parquet
        // REF: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#DECIMAL
        .add("c3a", DecimalType(9,3))
        .add("c3b", DecimalType(10,3))
        .add("c3c", DecimalType(20,3))
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
    val sortedCols = df.columns.sorted
    // Sort dataset by the first 2 columns (to minimize non-determinism in case multiple files have the same
    // value of the first column)
    df.select(sortedCols.head, sortedCols.tail: _*)
      .sort("c1_maxValue", "c1_minValue")
  }

}
