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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.{DataSourceWriteOptions, SparkAdapterSupport}
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.TaskContext
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FileFormat, FilePartition, PartitionedFile, SparkColumnarFileReader, UnsafeProjectionPool}
import org.apache.spark.sql.execution.datasources.parquet.TestRowProjectionReuse.{distinctObjects, drain, generationsOf, onNewThread, Read}
import org.apache.spark.sql.sources.{Filter, GreaterThanOrEqual, IsNotNull}
import org.apache.spark.sql.types.{ArrayType, DataType, DoubleType, IntegerType, LongType, MapType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.util.Collections
import java.util.concurrent.{Callable, Executors, TimeUnit}

import scala.collection.JavaConverters._

/**
 * The row-based parquet read path generates an UnsafeProjection for the rows of each file. For a wide nested schema
 * that generation is the largest per-file cost, and it produces the same projection for every file of a scan with the
 * same file schema. These tests check that the projection is generated once per thread and projection inputs rather
 * than once per file, and that reusing it changes no row.
 *
 * An UnsafeProjection returns the same UnsafeRow object for every row it projects, so the number of distinct row
 * objects a read returns is the number of projections it used.
 */
class TestRowProjectionReuse extends HoodieSparkClientTestBase with SparkAdapterSupport {

  private var spark: SparkSession = _

  private val nestedType = new StructType()
    .add("a", IntegerType)
    .add("arr", ArrayType(new StructType().add("x", LongType).add("y", StringType)))
    .add("m", MapType(StringType, new StructType().add("z", DoubleType)))

  private val partitionSchema = new StructType().add("p", StringType).add("q", IntegerType)

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initHoodieStorage()
  }

  /** Parquet files of (id int, v `vType`, s nestedType), one per entry, with `rowsPerFile` rows each. */
  private def writeFiles(vTypes: Seq[DataType], rowsPerFile: Int): Seq[(StoragePath, Long)] = {
    vTypes.zipWithIndex.map { case (vType, file) =>
      val schema = new StructType().add("id", IntegerType).add("v", vType).add("s", nestedType)
      val rows = (0 until rowsPerFile).map { i =>
        val id = file * 1000 + i
        // Boxed apart: an if over an Int and a Long would widen both to Long
        val v: Any = if (vType == IntegerType) Int.box(id * 7) else Long.box(id * 7L)
        val s = if (i % 5 == 3) null else Row(i, Seq(Row(id.toLong, s"y$id"), Row(-id.toLong, null)), Map(s"k$id" -> Row(id / 2.0)))
        Row(id, v, s)
      }
      val dir = s"$basePath/files/f$file"
      spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema).write.parquet(dir)
      val dataFile = storage.listDirectEntries(new StoragePath(dir)).asScala.find(_.getPath.getName.endsWith(".parquet")).get
      (dataFile.getPath, dataFile.getLength)
    }
  }

  private def rowBasedReader(): SparkColumnarFileReader =
    sparkAdapter.createParquetFileReader(vectorized = false, spark.sessionState.conf,
      Map(FileFormat.OPTION_RETURNING_BATCH -> "false"), spark.sessionState.newHadoopConf())

  private def partitionedFile(file: (StoragePath, Long), index: Int): PartitionedFile =
    sparkAdapter.getSparkPartitionedFileUtils.createPartitionedFile(
      InternalRow(UTF8String.fromString(s"p$index"), index), file._1, 0, file._2)

  @Test
  def testRowPathGeneratesOneProjectionForTheFilesOfAScan(): Unit = {
    val files = writeFiles(Seq.fill(6)(LongType), rowsPerFile = 10)
    val requiredSchema = new StructType().add("id", IntegerType).add("v", LongType).add("s", nestedType)
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val reader = rowBasedReader()
    val storageConf = HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())

    val reads = onNewThread {
      files.zipWithIndex.map { case (file, i) =>
        drain(reader.read(partitionedFile(file, i), requiredSchema, partitionSchema, HOption.empty(), Seq.empty, storageConf), outputSchema)
      }
    }
    assertEquals(1, distinctObjects(reads.flatMap(_.rowObjects)), "One projection for the six files of the scan")
    // Every file still gets its own partition values.
    reads.zipWithIndex.foreach { case (read, i) =>
      assertEquals(10, read.rows.size)
      read.rows.foreach(row => assertEquals(Seq(s"p$i", i), row.toSeq.takeRight(2)))
    }
  }

  /**
   * Rows of a scan are not copied: a reader returns the same UnsafeRow object for every row of a file, valid until the
   * next call on that iterator. Iterators of the same projection key that are open at the same time therefore must not
   * share a projection, and a projection may only move to another iterator once its own has no more rows.
   */
  @Test
  def testUncopiedRowsOfOpenIteratorsKeepTheirValues(): Unit = {
    val files = writeFiles(Seq.fill(3)(LongType), rowsPerFile = 5)
    val requiredSchema = new StructType().add("id", IntegerType).add("v", LongType).add("s", nestedType)
    val reader = rowBasedReader()
    val storageConf = HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())
    def open(i: Int): Iterator[InternalRow] =
      reader.read(partitionedFile(files(i), i), requiredSchema, partitionSchema, HOption.empty(), Seq.empty, storageConf)

    onNewThread {
      val a = open(0)
      val b = open(1)
      // Both files need the same projection, and both iterators are open: each has its own.
      val rowA = a.next()
      val rowB = b.next()
      assertTrue(rowA ne rowB, "Open iterators of the same key share a projection")
      assertEquals(0, rowA.getInt(0))
      assertEquals(1000, rowB.getInt(0))
      // Advancing one iterator rewrites its own row object only.
      assertEquals(1001, b.next().getInt(0))
      assertEquals(1001, rowB.getInt(0))
      assertEquals(0, rowA.getInt(0), "An uncopied row of one file was overwritten by the rows of another")

      // Drain a, keeping its last row without copying it, and a copy of it.
      var lastA = rowA
      while (a.hasNext) {
        lastA = a.next()
      }
      val lastACopy = lastA.copy()
      assertEquals(4, lastACopy.getInt(0))

      // Only now, a being exhausted, does its projection move to the next file of that key...
      val c = open(2)
      val rowC = c.next()
      assertTrue(rowC eq lastA, "The next iterator of the key takes the projection of the exhausted one")
      assertEquals(2000, rowC.getInt(0))
      // ...which writes into the row object that a returned last. That is the row contract of a scan (a row is valid
      // until the next call on its iterator), and why the projection is not handed on before a says it has no rows.
      assertEquals(2000, lastA.getInt(0))
      assertEquals(4, lastACopy.getInt(0))
      // b, still open, keeps its own projection and row.
      assertEquals(1001, rowB.getInt(0))
      assertEquals(1002, b.next().getInt(0))
      assertEquals(2000, rowC.getInt(0))
    }
  }

  /**
   * The projection of a type-changed file carries the SQL configs in its key. Two queries differ in their task local
   * properties (spark.sql.execution.id, the job description), which are not SQL configs, so the second query reuses
   * the projection of the first.
   */
  @Test
  def testTypeChangedFilesReuseTheirProjectionAcrossQueries(): Unit = {
    val files = writeFiles(Seq(IntegerType), rowsPerFile = 5)
    val requiredSchema = new StructType().add("id", IntegerType).add("v", LongType).add("s", nestedType)
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val reader = rowBasedReader()
    val storageConf = HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())
    val keyClasses = Seq(classOf[RowProjectionKey])

    val (reads, generated) = onNewThread {
      val start = generationsOf(keyClasses).head
      val reads = Seq("1", "2").map { executionId =>
        val taskContext = TaskContext.empty()
        taskContext.getLocalProperties.setProperty("spark.sql.execution.id", executionId)
        taskContext.getLocalProperties.setProperty("spark.job.description", s"query $executionId")
        TaskContext.setTaskContext(taskContext)
        drain(reader.read(partitionedFile(files.head, 0), requiredSchema, partitionSchema, HOption.empty(), Seq.empty, storageConf),
          outputSchema)
      }
      (reads, generationsOf(keyClasses).head - start)
    }
    assertEquals(1L, generated, "The second query reuses the projection of the type-changed file")
    assertEquals(1, distinctObjects(reads.flatMap(_.rowObjects)))
    assertEquals(reads.head.rows, reads(1).rows)
    assertEquals(0L, reads.head.rows.head.getLong(1))
  }

  @Test
  def testReusedProjectionsReturnTheRowsOfFreshOnes(): Unit = {
    // Files that store v as an int are read as long through a cast; the others need none. Two projections.
    val vTypes = Seq(IntegerType, LongType, IntegerType, LongType, IntegerType, LongType)
    val files = writeFiles(vTypes, rowsPerFile = 20)
    val requiredSchema = new StructType().add("id", IntegerType).add("v", LongType).add("s", nestedType)
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val reader = rowBasedReader()
    val storageConf = HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())
    val filters: Seq[Filter] = Seq(IsNotNull("id"), GreaterThanOrEqual("v", 0L))
    def read(i: Int): Read =
      drain(reader.read(partitionedFile(files(i), i), requiredSchema, partitionSchema, HOption.empty(), filters, storageConf), outputSchema)

    // Uncached: each file on a thread of its own generates its projection.
    val fresh = files.indices.map(i => onNewThread(read(i)))
    // Cached: all files twice on one thread, alternating between the two projections.
    val reused = onNewThread(files.indices.map(read) ++ files.indices.map(read))

    assertEquals(2, distinctObjects(reused.flatMap(_.rowObjects)), "One projection per file schema")
    files.indices.foreach { i =>
      assertEquals(20, fresh(i).rows.size)
      assertEquals(fresh(i).rows, reused(i).rows)
      assertEquals(fresh(i).rows, reused(files.size + i).rows)
    }
    assertEquals(7L * 1000, fresh(1).rows(0).getLong(1), "The long file of index 1 holds v = 7 * id")
    assertEquals(7L * 2000, fresh(2).rows(0).getLong(1), "The int file of index 2 is cast to long")
  }

  @Test
  def testThreadsReadingOneReaderUseTheirOwnProjections(): Unit = {
    val vTypes = Seq(IntegerType, LongType, LongType, IntegerType)
    val files = writeFiles(vTypes, rowsPerFile = 200)
    val requiredSchema = new StructType().add("id", IntegerType).add("v", LongType).add("s", nestedType)
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    // One reader for all threads, as the tasks of an executor share the broadcast reader.
    val reader = rowBasedReader()
    val storageConf = HadoopFSUtils.getStorageConf(spark.sessionState.newHadoopConf())
    val expected = files.indices.map { i =>
      onNewThread(drain(reader.read(partitionedFile(files(i), i), requiredSchema, partitionSchema, HOption.empty(), Seq.empty, storageConf),
        outputSchema)).rows
    }

    val threads = 6
    val rounds = 5
    val executor = Executors.newFixedThreadPool(threads)
    try {
      val results = (0 until threads).map { t =>
        executor.submit(new Callable[Seq[(Int, Read)]] {
          override def call(): Seq[(Int, Read)] = {
            TaskContext.setTaskContext(TaskContext.empty())
            try {
              // Each thread reads the files in its own order, several times.
              (0 until rounds).flatMap(round => files.indices.map(i => (i + t + round) % files.size)).map { i =>
                // Rows are compared as they are read: a projection shared with another thread would show here.
                (i, drain(reader.read(partitionedFile(files(i), i), requiredSchema, partitionSchema, HOption.empty(), Seq.empty, storageConf),
                  outputSchema))
              }
            } finally {
              TaskContext.unset()
            }
          }
        })
      }.map(_.get(5, TimeUnit.MINUTES))

      results.foreach { reads =>
        reads.foreach { case (i, read) => assertEquals(expected(i), read.rows) }
        assertEquals(2, distinctObjects(reads.flatMap(_._2.rowObjects)), "Each thread reuses its two projections")
      }
      assertEquals(threads * 2, distinctObjects(results.flatten.flatMap(_._2.rowObjects)), "No projection is shared between threads")
    } finally {
      executor.shutdownNow()
    }
  }

  /**
   * The whole Hudi read function over a partitioned table whose base files hold `v` as int or long, with or without
   * schema-on-read, with pushed filters and, on MOR, file slices merged with log files: reading all files on one
   * thread reuses projections and returns the rows that reading each file on a thread of its own returns.
   */
  @ParameterizedTest
  @CsvSource(Array("COPY_ON_WRITE,false", "COPY_ON_WRITE,true", "MERGE_ON_READ,false", "MERGE_ON_READ,true"))
  def testHudiReadReturnsTheRowsOfFreshProjections(tableType: HoodieTableType, schemaOnRead: Boolean): Unit = {
    val opts = Map(
      HoodieWriteConfig.TBL_NAME.key -> "row_projection_reuse",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name,
      DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "p",
      HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
      HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key -> schemaOnRead.toString,
      HoodieCompactionConfig.INLINE_COMPACT.key -> "false",
      // A new file group per insert, so that the table keeps base files of both types.
      "hoodie.parquet.small.file.limit" -> "0",
      "hoodie.insert.shuffle.parallelism" -> "2",
      "hoodie.upsert.shuffle.parallelism" -> "2")
    def write(vType: DataType, ids: Range, ts: Long, operation: String): Unit = {
      val schema = new StructType().add("id", StringType).add("p", StringType).add("ts", LongType).add("v", vType).add("s", nestedType)
      val rows = ids.map { id =>
        val v: Any = if (vType == IntegerType) Int.box(id * 7 + ts.toInt) else Long.box(id * 7L + ts)
        Row(s"id$id", s"p${id % 2}", ts, v, Row(id, Seq(Row(id.toLong, s"y$id")), Map(s"k$id" -> Row(ts.toDouble))))
      }
      spark.createDataFrame(spark.sparkContext.parallelize(rows, 2), schema).write.format("hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, operation)
        .mode(if (ts == 1L) SaveMode.Overwrite else SaveMode.Append)
        .save(basePath)
    }
    write(IntegerType, 0 until 20, 1L, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    write(LongType, 20 until 40, 2L, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
    // Updates in p0 only: COW rewrites those file groups as long, MOR adds log files; p1 keeps its int base file.
    write(LongType, (0 until 40 by 4), 3L, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)

    spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    try {
      val df = spark.read.format("hudi").option(HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.key, schemaOnRead.toString)
        .load(basePath).where("v >= 0")
      val scan = df.queryExecution.sparkPlan.collectFirst { case scan: FileSourceScanExec => scan }.get
      val relation = scan.relation
      val files = scan.inputRDD.partitions.flatMap(_.asInstanceOf[FilePartition].files).toSeq
      val readFunction = relation.fileFormat.buildReaderWithPartitionValues(spark, relation.dataSchema,
        relation.partitionSchema, scan.requiredSchema, Seq(GreaterThanOrEqual("v", 0L)),
        relation.options + (FileFormat.OPTION_RETURNING_BATCH -> "false"), spark.sessionState.newHadoopConfWithOptions(relation.options))
      val outputSchema = StructType(scan.requiredSchema.fields ++ relation.partitionSchema.fields)
      assertTrue(files.size >= 4, s"Expected base files of both types, got ${files.size} files")

      // Read inside a task, as an executor does: the read function takes its reader from a broadcast.
      val keyClasses = Seq(classOf[RowProjectionKey], classOf[ByNameProjectionKey], classOf[NestedPruningProjectionKey])
      val (fresh, reused, projections, roundGenerations) = spark.sparkContext.parallelize(Seq(0), 1).mapPartitions { _ =>
        val taskContext = TaskContext.get()
        val fresh = files.map(file => onNewThread(drain(readFunction(file), outputSchema).rows, taskContext))
        // Two rounds on one thread, as two scans of the table in later tasks on one executor thread would be.
        val (reused, roundGenerations) = onNewThread({
          val start = generationsOf(keyClasses)
          val first = files.map(file => drain(readFunction(file), outputSchema))
          val afterFirst = generationsOf(keyClasses)
          val second = files.map(file => drain(readFunction(file), outputSchema))
          val afterSecond = generationsOf(keyClasses)
          (first ++ second, (afterFirst.zip(start).map(p => p._1 - p._2), afterSecond.zip(afterFirst).map(p => p._1 - p._2)))
        }, taskContext)
        Iterator((fresh, reused.map(_.rows), distinctObjects(reused.flatMap(_.rowObjects)), roundGenerations))
      }.collect().head

      files.indices.foreach { i =>
        assertEquals(fresh(i), reused(i))
        assertEquals(fresh(i), reused(files.size + i))
      }
      val (firstRound, secondRound) = roundGenerations
      // The base files are read row-based in both table types (MOR file slices with log files through the file group
      // reader), and the second scan generates nothing: every projection it needs was kept from the first.
      assertTrue(firstRound.head > 0, s"The parquet reader generated no row projection: $firstRound")
      assertTrue(firstRound.head <= files.size, s"More row projections than files: $firstRound")
      assertEquals(Seq(0L, 0L, 0L), secondRound, s"The second scan generated projections (first scan: $firstRound)")
      assertEquals(40, fresh.map(_.size).sum)
      assertTrue(projections < files.size, s"Expected fewer projections than the ${files.size} files, got $projections")
      // The updated values come through, and int base files are read as long.
      val vById = fresh.flatten.map(row => row.getAs[String](outputSchema.fieldIndex("id")) -> row.getAs[Long](outputSchema.fieldIndex("v"))).toMap
      assertEquals(4 * 7L + 3, vById("id4"))
      assertEquals(1 * 7L + 1, vById("id1"))
      assertEquals(21 * 7L + 2, vById("id21"))
    } finally {
      spark.conf.unset("spark.sql.parquet.enableVectorizedReader")
    }
  }
}

object TestRowProjectionReuse {

  /** The projections generated so far for each of `keyClasses`, on all threads. */
  def generationsOf(keyClasses: Seq[Class[_]]): Seq[Long] = keyClasses.map(UnsafeProjectionPool.generations)

  /** The rows of one read, as Scala rows, and the row objects it returned. */
  case class Read(rows: Seq[Row], rowObjects: Seq[InternalRow])

  def drain(iter: Iterator[InternalRow], outputSchema: StructType): Read = {
    val toScala = CatalystTypeConverters.createToScalaConverter(outputSchema)
    val rows = Seq.newBuilder[Row]
    val rowObjects = Seq.newBuilder[InternalRow]
    while (iter.hasNext) {
      val row = iter.next()
      rowObjects += row
      rows += toScala(row.copy()).asInstanceOf[Row]
    }
    Read(rows.result(), rowObjects.result())
  }

  def distinctObjects(rowObjects: Seq[InternalRow]): Int = {
    val distinct = Collections.newSetFromMap(new java.util.IdentityHashMap[InternalRow, java.lang.Boolean]())
    rowObjects.foreach(distinct.add)
    distinct.size()
  }

  /**
   * Runs `body` on a new thread, which starts with no reusable projection, as a task on it would. The thread runs
   * under `taskContext`, a placeholder unless the caller runs in a task.
   */
  def onNewThread[T](body: => T, taskContext: => TaskContext = TaskContext.empty()): T = {
    val executor = Executors.newSingleThreadExecutor()
    try {
      executor.submit(new Callable[T] {
        override def call(): T = {
          TaskContext.setTaskContext(taskContext)
          try body finally TaskContext.unset()
        }
      }).get(5, TimeUnit.MINUTES)
    } finally {
      executor.shutdownNow()
    }
  }
}
