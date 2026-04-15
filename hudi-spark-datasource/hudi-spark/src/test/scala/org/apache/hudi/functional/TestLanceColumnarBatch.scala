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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.DefaultSparkRecordMerger
import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.client.SparkTaskContextSupplier
import org.apache.hudi.common.bloom.{BloomFilterFactory, BloomFilterTypeCode}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.io.storage.HoodieSparkLanceWriter
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, QueryStageExec}
import org.apache.spark.sql.execution.datasources.lance.SparkLanceReaderBase
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.condition.DisabledIfSystemProperty

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * Tests for Lance columnar batch (vectorized) reading, covering:
 *
 * Unit-level tests (invoke [[SparkLanceReaderBase]] directly):
 *  - Row-based path: `enableVectorizedReader=false` must yield [[InternalRow]], never [[ColumnarBatch]]
 *  - Columnar path: `enableVectorizedReader=true` with matching schemas yields [[ColumnarBatch]]
 *  - Null-padding: columns absent from the file are null-padded in the [[ColumnarBatch]]
 *  - Partition vectors: partition values are appended as constant columns to each [[ColumnarBatch]]
 *  - Fallback: implicit type change (e.g. FLOAT → DOUBLE) forces the row path
 *
 * Integration tests (Spark SQL / DataFrame API):
 *  - COW table round-trip via `spark.read.format("hudi")` with vectorized reads active
 *  - Schema evolution: adding a column via bulk_insert; old base files are null-padded
 *  - SQL query: `SELECT` with a `WHERE` clause on a COW table
 */
@DisabledIfSystemProperty(named = "lance.skip.tests", matches = "true")
class TestLanceColumnarBatch extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    spark = sqlContext.sparkSession
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    spark = null
  }

  /**
   * Write a plain Lance file (no Hudi metadata fields) containing [[rows]] with [[schema]].
   * Returns the absolute path string of the written file.
   */
  private def writeLanceFile(fileName: String, schema: StructType, rows: Seq[InternalRow]): String = {
    val bloom = BloomFilterFactory.createBloomFilter(1000, 0.0001, 10000, BloomFilterTypeCode.SIMPLE.name())
    val path = new StoragePath(s"$basePath/$fileName")
    val storage = HoodieTestUtils.getStorage(basePath)
    try {
      val writer = HoodieSparkLanceWriter.builder
        .file(path).sparkSchema(schema)
        .instantTime("20240101120000000")
        .taskContextSupplier(new SparkTaskContextSupplier())
        .storage(storage)
        .populateMetaFields(false)
        .bloomFilterOpt(HOption.of(bloom)).build
      try {
        rows.zipWithIndex.foreach { case (row, i) => writer.writeRow("key" + i, row) }
      } finally {
        writer.close()
      }
    } finally {
      storage.close()
    }
    path.toString
  }

  private def extractRow(row: InternalRow, schema: StructType): (Int, String, Any) = {
    val id = row.getInt(0)
    val name = if (row.isNullAt(1)) null else row.getUTF8String(1).toString
    val col2 = if (schema.fields.length >= 3) {
      val f = schema.fields(2)
      if (row.isNullAt(2)) null
      else f.dataType match {
        case DoubleType  => row.getDouble(2).asInstanceOf[AnyRef]
        case FloatType   => row.getFloat(2).asInstanceOf[AnyRef]
        case IntegerType => row.getInt(2).asInstanceOf[AnyRef]
        case LongType    => row.getLong(2).asInstanceOf[AnyRef]
        case StringType  => row.getUTF8String(2).toString
        case _           => row.get(2, f.dataType)
      }
    } else null
    (id, name, col2)
  }

  /**
   * Invoke [[SparkLanceReaderBase.read]] and consume the iterator, safely extracting data from
   * each item before the iterator advances (which would invalidate Arrow-backed batch buffers).
   *
   * Returns a tuple of:
   *  - `batchCount`  – number of [[ColumnarBatch]] items returned
   *  - `rowCount`    – number of [[InternalRow]] items returned (non-batch path)
   *  - `rows`        – collected (id: Int, name: String, col2: Any) tuples from both paths
   */
  private def readAndCollect(
      filePath: String,
      requiredSchema: StructType,
      partitionSchema: StructType = new StructType(),
      partitionValues: InternalRow = new GenericInternalRow(Array.empty[Any]),
      enableVectorized: Boolean = true
  ): (Int, Int, Seq[(Int, String, Any)]) = {

    val pf = sparkAdapter.getSparkPartitionedFileUtils
      .createPartitionedFile(partitionValues, new StoragePath(filePath), 0L, Long.MaxValue)
    val storageConf = new HadoopStorageConfiguration(new Configuration())
    val reader = new SparkLanceReaderBase(enableVectorized)

    var batchCount = 0
    var rowCount = 0
    val rows = ArrayBuffer.empty[(Int, String, Any)]

    // Cast to Iterator[Any] to avoid JVM checkcast to InternalRow.
    // The columnar batch path returns ColumnarBatch via type erasure (Iterator[InternalRow]
    // that actually contains ColumnarBatch). Both iter.foreach and iter.next() on a typed
    // Iterator[InternalRow] insert a checkcast that fails for ColumnarBatch.
    val iter = reader.read(pf, requiredSchema, partitionSchema, HOption.empty(), Seq.empty[Filter], storageConf)
      .asInstanceOf[Iterator[Any]]

    try {
      while (iter.hasNext) {
        iter.next() match {
          case batch: ColumnarBatch =>
            batchCount += 1
            batch.rowIterator().asScala.foreach { row =>
              rows += extractRow(row, requiredSchema)
            }

          case row: InternalRow =>
            rowCount += 1
            rows += extractRow(row, requiredSchema)
        }
      }
    } finally {
      // On the driver in unit tests TaskContext.get() is null, so the
      // completion-listener cleanup path inside readBatch() never fires.
      // Explicitly close to release Arrow allocators and file readers.
      iter match {
        case c: AutoCloseable => c.close()
        case _ => // row path returns a plain Iterator
      }
    }

    (batchCount, rowCount, rows.toSeq)
  }

  /**
   * Assert that the executed plan for `df` used a columnar (vectorized) scan and
   * that the scan's `numOutputRows` SQLMetric is populated. This guards against a
   * silent fallback to the row reader that would still produce correct output.
   *
   * The metric is lazy: the caller must have already forced execution (e.g. via
   * `df.collect()`) before invoking this helper.
   */
  private def assertLanceVectorizedScan(df: DataFrame, expectedMinRows: Long): Unit = {
    val rootPlan = df.queryExecution.executedPlan

    def allNodes(p: SparkPlan): Seq[SparkPlan] = {
      val hidden: Seq[SparkPlan] = p match {
        case aqe: AdaptiveSparkPlanExec => allNodes(aqe.executedPlan)
        case qs: QueryStageExec => allNodes(qs.plan)
        case _ => Seq.empty
      }
      (p +: p.children.flatMap(allNodes)) ++ hidden
    }

    val nodes = allNodes(rootPlan)
    val scanOpt = nodes.collectFirst {
      case s: SparkPlan if s.supportsColumnar &&
        (s.getClass.getSimpleName.contains("FileSourceScan") ||
         s.getClass.getSimpleName.contains("BatchScan")) => s
    }
    assertTrue(scanOpt.isDefined,
      s"Expected a columnar (supportsColumnar=true) scan node in executed plan, found none:\n${rootPlan.treeString}")
    val scan = scanOpt.get
    val numOutputRows = scan.metrics.get("numOutputRows").map(_.value).getOrElse(-1L)
    assertTrue(numOutputRows >= expectedMinRows,
      s"Columnar Lance scan produced $numOutputRows rows, expected >= $expectedMinRows; plan:\n${rootPlan.treeString}")
  }

  private val baseSchema: StructType = new StructType()
    .add("id", IntegerType, nullable = false)
    .add("name", StringType, nullable = true)
    .add("score", DoubleType, nullable = true)

  private def baseRows: Seq[InternalRow] = Seq(
    new GenericInternalRow(Array[Any](1, UTF8String.fromString("Alice"), 95.5d)),
    new GenericInternalRow(Array[Any](2, UTF8String.fromString("Bob"), 87.3d)),
    new GenericInternalRow(Array[Any](3, UTF8String.fromString("Charlie"), 92.1d))
  )

  private def writeHudiTable(tableType: HoodieTableType,
                              tableName: String,
                              tablePath: String,
                              df: DataFrame,
                              saveMode: SaveMode = SaveMode.Append,
                              operation: Option[String] = None,
                              extraOptions: Map[String, String] = Map.empty): Unit = {
    var writer = df.write
      .format("hudi")
      .option("hoodie.table.base.file.format", "LANCE")
      .option(TABLE_TYPE.key(), tableType.name())
      .option(RECORDKEY_FIELD.key(), "id")
      .option(PRECOMBINE_FIELD.key(), "id")
      .option(TABLE_NAME.key(), tableName)
      .option(HoodieWriteConfig.TBL_NAME.key(), tableName)
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), classOf[DefaultSparkRecordMerger].getName)
    operation.foreach(op => writer = writer.option(OPERATION.key(), op))
    extraOptions.foreach { case (k, v) => writer = writer.option(k, v) }
    writer.mode(saveMode).save(tablePath)
  }

  // ===========================================================================
  // Unit tests: SparkLanceReaderBase
  // ===========================================================================

  /**
   * Row path (`enableVectorizedReader=false`) must return [[InternalRow]] instances, never
   * [[ColumnarBatch]].  Data values must survive the row-level UnsafeProjection round-trip.
   */
  @Test
  def testRowPathReturnsInternalRows(): Unit = {
    val filePath = writeLanceFile("row_path.lance", baseSchema, baseRows)
    val (batches, rowItems, rows) = readAndCollect(filePath, baseSchema, enableVectorized = false)

    assertEquals(0, batches, "Row path must not yield ColumnarBatch")
    assertEquals(3, rowItems, "Row path must yield one InternalRow per record")

    assertEquals(List(1, 2, 3), rows.map(_._1))
    assertEquals(List("Alice", "Bob", "Charlie"), rows.map(_._2))
    assertEquals(List(95.5d, 87.3d, 92.1d), rows.map(_._3))
  }

  /**
   * Columnar path (`enableVectorizedReader=true`, no type changes) must return
   * [[ColumnarBatch]] instances backed by [[org.apache.spark.sql.vectorized.LanceArrowColumnVector]].
   * No [[InternalRow]] may be returned directly.  Data must round-trip correctly.
   */
  @Test
  def testColumnarPathReturnsBatches(): Unit = {
    val filePath = writeLanceFile("columnar_path.lance", baseSchema, baseRows)
    val (batches, rowItems, rows) = readAndCollect(filePath, baseSchema, enableVectorized = true)

    assertTrue(batches > 0, "Vectorized path must yield at least one ColumnarBatch")
    assertEquals(0, rowItems, "Vectorized path must not yield raw InternalRow")

    assertEquals(3, rows.size)
    assertEquals(List(1, 2, 3), rows.map(_._1))
    assertEquals(List("Alice", "Bob", "Charlie"), rows.map(_._2))
    assertEquals(List(95.5d, 87.3d, 92.1d), rows.map(_._3))
  }

  /**
   * When the required schema contains a column that is absent from the file (schema evolution:
   * column addition), the vectorized path must null-pad that column in each [[ColumnarBatch]]
   * using a [[org.apache.spark.sql.vectorized.LanceArrowColumnVector]] backed by an all-null
   * Arrow vector.
   */
  @Test
  def testColumnarPathNullPadsAbsentColumns(): Unit = {
    val fileSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)
    val rows = Seq(
      new GenericInternalRow(Array[Any](1, UTF8String.fromString("Alice"))),
      new GenericInternalRow(Array[Any](2, UTF8String.fromString("Bob")))
    )
    val filePath = writeLanceFile("null_pad.lance", fileSchema, rows)

    // requiredSchema adds 'score' which does not exist in the file
    val requiredSchemaWithExtra = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)
      .add("score", DoubleType, nullable = true)

    val pf = sparkAdapter.getSparkPartitionedFileUtils
      .createPartitionedFile(new GenericInternalRow(Array.empty[Any]),
        new StoragePath(filePath), 0L, Long.MaxValue)
    val storageConf = new HadoopStorageConfiguration(new Configuration())
    val reader = new SparkLanceReaderBase(enableVectorizedReader = true)

    var batchesSeen = 0
    val nullPadded = ArrayBuffer.empty[Boolean]

    val iter = reader.read(pf, requiredSchemaWithExtra, new StructType(), HOption.empty(),
      Seq.empty[Filter], storageConf).asInstanceOf[Iterator[Any]]
    while (iter.hasNext) {
      iter.next() match {
        case batch: ColumnarBatch =>
          batchesSeen += 1
          batch.rowIterator().asScala.foreach { row =>
            nullPadded += row.isNullAt(2)
          }
        case _ =>
          fail("Vectorized path with null-padding must return ColumnarBatch")
      }
    }

    assertTrue(batchesSeen > 0, "Must have produced at least one ColumnarBatch")
    assertEquals(2, nullPadded.size, "Must have read 2 rows")
    assertTrue(nullPadded.forall(identity), "Absent column 'score' must be null in every row")
  }

  /**
   * When a non-empty `partitionSchema` is supplied, the vectorized path must append constant
   * partition-value columns to each [[ColumnarBatch]] using
   * [[org.apache.spark.sql.execution.vectorized.OnHeapColumnVector]].
   *
   * The resulting rows must have `dataSchema.size + partitionSchema.size` fields, with the
   * partition value in the last position.
   */
  @Test
  def testColumnarPathAppendsPartitionVectors(): Unit = {
    val filePath = writeLanceFile("partitioned.lance", baseSchema, baseRows)

    val partitionSchema = new StructType().add("dept", StringType, nullable = true)
    val partitionValues = new GenericInternalRow(
      Array[Any](UTF8String.fromString("engineering")))

    val pf = sparkAdapter.getSparkPartitionedFileUtils
      .createPartitionedFile(partitionValues, new StoragePath(filePath), 0L, Long.MaxValue)
    val storageConf = new HadoopStorageConfiguration(new Configuration())
    val reader = new SparkLanceReaderBase(enableVectorizedReader = true)

    var batchesSeen = 0
    val depts = ArrayBuffer.empty[String]

    val iter = reader.read(pf, baseSchema, partitionSchema, HOption.empty(),
      Seq.empty[Filter], storageConf).asInstanceOf[Iterator[Any]]
    while (iter.hasNext) {
      iter.next() match {
        case batch: ColumnarBatch =>
          batchesSeen += 1
          assertEquals(baseSchema.size + partitionSchema.size, batch.numCols(),
            "Batch column count must equal data + partition columns")
          batch.rowIterator().asScala.foreach { row =>
            depts += row.getUTF8String(baseSchema.size).toString
          }
        case _ =>
          fail("Vectorized path must return ColumnarBatch when partitionSchema is present")
      }
    }

    assertTrue(batchesSeen > 0)
    assertEquals(3, depts.size)
    assertTrue(depts.forall(_ == "engineering"),
      "Every row must carry the constant partition value 'engineering'")
  }

  /**
   * When [[SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo]] detects an implicit type
   * change (file has FLOAT, query requires DOUBLE), the columnar path must fall back to the
   * row path.  The row path applies a cast projection so the returned values must be correct
   * DOUBLE values even though the file stores FLOAT.
   */
  @Test
  def testTypeChangeFallsBackToRowPath(): Unit = {
    val fileSchema = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)
      .add("score", FloatType, nullable = true)

    val fileRows = Seq(
      new GenericInternalRow(Array[Any](1, UTF8String.fromString("Alice"), 95.5f)),
      new GenericInternalRow(Array[Any](2, UTF8String.fromString("Bob"), 87.3f))
    )
    val filePath = writeLanceFile("type_change.lance", fileSchema, fileRows)

    // requiredSchema uses DOUBLE for 'score' — implicit type change: FLOAT → DOUBLE
    val requiredSchemaDoubleScore = new StructType()
      .add("id", IntegerType, nullable = false)
      .add("name", StringType, nullable = true)
      .add("score", DoubleType, nullable = true)

    val (batches, rowItems, rows) = readAndCollect(filePath, requiredSchemaDoubleScore, enableVectorized = true)

    assertEquals(0, batches,
      "Implicit type change FLOAT→DOUBLE must disable the columnar path and return InternalRow")
    assertEquals(2, rowItems)

    assertEquals(List(1, 2), rows.map(_._1))
    assertEquals(List("Alice", "Bob"), rows.map(_._2))
    // FLOAT 95.5f → cast via String → DOUBLE: result must be close to 95.5
    assertEquals(95.5d, rows(0)._3.asInstanceOf[Double], 0.01d)
    assertEquals(87.3d, rows(1)._3.asInstanceOf[Double], 0.01d)
  }

  // ===========================================================================
  // Integration tests: Spark SQL / DataFrame API (COW only)
  // ===========================================================================

  /**
   * End-to-end test for a COW Lance table read via `spark.read.format("hudi")`.
   *
   * With `lanceBatchSupported = true`, Spark will enable vectorized reads for Lance COW tables.
   * This test verifies that data written via the Hudi DataFrame API is read back correctly when
   * the vectorized code path is active.
   */
  @Test
  def testCOWTableDataFrameRead(): Unit = {
    val tableName = "test_lance_columnar_cow"
    val tablePath = s"$basePath/$tableName"

    val df = spark.createDataFrame(Seq(
      (1, "Alice", 95.5d),
      (2, "Bob", 87.3d),
      (3, "Charlie", 92.1d)
    )).toDF("id", "name", "score")

    writeHudiTable(HoodieTableType.COPY_ON_WRITE, tableName, tablePath, df,
      saveMode = SaveMode.Overwrite, operation = Some("bulk_insert"))

    val actual = spark.read.format("hudi").load(tablePath)
      .select("id", "name", "score")
      .orderBy("id")

    val expected = Seq(
      (1, "Alice", 95.5d),
      (2, "Bob", 87.3d),
      (3, "Charlie", 92.1d)
    )

    val actualRows = actual.collect()
    assertEquals(3, actualRows.length)
    expected.zip(actualRows).foreach { case ((eid, ename, escore), row) =>
      assertEquals(eid, row.getAs[Int]("id"))
      assertEquals(ename, row.getAs[String]("name"))
      assertEquals(escore, row.getAs[Double]("score"), 1e-6)
    }

    assertLanceVectorizedScan(actual, 3)
  }

  /**
   * Schema evolution: a second bulk_insert writes records with a wider schema that adds a
   * 'department' column.  Old base files (from the first insert) do not contain 'department'.
   *
   * When reading the whole table with the full schema, `SparkLanceReaderBase` must null-pad
   * 'department' for every row from the old base file, while the new base file has actual values.
   */
  @Test
  def testCOWTableSchemaEvolutionNullPadding(): Unit = {
    val tableName = "test_lance_schema_evolution"
    val tablePath = s"$basePath/$tableName"

    // First batch: narrow schema (id, name, score)
    val df1 = spark.createDataFrame(Seq(
      (1, "Alice", 95.5d),
      (2, "Bob", 87.3d)
    )).toDF("id", "name", "score")

    writeHudiTable(HoodieTableType.COPY_ON_WRITE, tableName, tablePath, df1,
      saveMode = SaveMode.Overwrite, operation = Some("bulk_insert"))

    // Second batch: wider schema adds 'department'
    val df2 = spark.createDataFrame(Seq(
      (3, "Charlie", 92.1d, "engineering"),
      (4, "David", 88.0d, "sales")
    )).toDF("id", "name", "score", "department")

    writeHudiTable(HoodieTableType.COPY_ON_WRITE, tableName, tablePath, df2,
      operation = Some("bulk_insert"),
      extraOptions = Map(RECONCILE_SCHEMA.key() -> "true"))

    // Read back with full schema — 'department' must be null for records from the first batch
    val readDf = spark.read.format("hudi").load(tablePath)
    val actual = readDf.select("id", "name", "score", "department").orderBy("id")
    val rows = actual.collect()

    assertEquals(4, rows.length)

    // Records 1 and 2 came from the narrow-schema file → 'department' must be null
    assertEquals(1, rows(0).getAs[Int]("id"))
    assertTrue(rows(0).isNullAt(rows(0).fieldIndex("department")),
      "Records from the narrow-schema file must have null 'department'")
    assertEquals(2, rows(1).getAs[Int]("id"))
    assertTrue(rows(1).isNullAt(rows(1).fieldIndex("department")),
      "Records from the narrow-schema file must have null 'department'")

    // Records 3 and 4 came from the wider-schema file → 'department' must be present
    assertEquals(3, rows(2).getAs[Int]("id"))
    assertEquals("engineering", rows(2).getAs[String]("department"))
    assertEquals(4, rows(3).getAs[Int]("id"))
    assertEquals("sales", rows(3).getAs[String]("department"))

    // Confirm the vectorized scan path was used for at least one of the scanned files.
    assertLanceVectorizedScan(actual, 1)
  }

  /**
   * Spark SQL `SELECT … WHERE` query on a COW Lance table.  Verifies that predicate evaluation
   * works correctly when vectorized reading is active (Spark evaluates predicates on
   * [[ColumnarBatch]] rows returned from the vectorized scan).
   */
  @Test
  def testCOWTableSparkSqlQuery(): Unit = {
    val tableName = "test_lance_sql_columnar"
    val tablePath = s"$basePath/$tableName"

    val df = spark.createDataFrame(Seq(
      (1, "Alice", 30),
      (2, "Bob", 25),
      (3, "Charlie", 35),
      (4, "David", 28),
      (5, "Eve", 32)
    )).toDF("id", "name", "age")

    writeHudiTable(HoodieTableType.COPY_ON_WRITE, tableName, tablePath, df,
      saveMode = SaveMode.Overwrite, operation = Some("bulk_insert"))

    spark.read.format("hudi").load(tablePath).createOrReplaceTempView(tableName)

    val resultDf = spark.sql(
      s"SELECT id, name, age FROM $tableName WHERE age > 30 ORDER BY id"
    )
    val result = resultDf.collect()

    assertEquals(2, result.length, "Only Charlie (35) and Eve (32) satisfy age > 30")
    assertEquals(3, result(0).getAs[Int]("id"))
    assertEquals("Charlie", result(0).getAs[String]("name"))
    assertEquals(5, result(1).getAs[Int]("id"))
    assertEquals("Eve", result(1).getAs[String]("name"))

    // `numOutputRows` on the scan reflects pre-filter rows; all 5 rows flow through the columnar scan.
    assertLanceVectorizedScan(resultDf, 1)
  }
}
