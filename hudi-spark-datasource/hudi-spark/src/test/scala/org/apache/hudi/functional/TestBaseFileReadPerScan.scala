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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.functional.TestBaseFileReadPerScan.{countDefaultResourceLoads, dataFileReadConfs, fileScan, readInTasks, recordReadConfs}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, LocalFileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasources.{FileFormat, FileScanRDD, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.net.URL
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger

/**
 * Work a copy-on-write scan does once per scan rather than once per base file: the parquet read conf, and the table
 * schema conversion for logical timestamp repair.
 */
class TestBaseFileReadPerScan extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  private val writeOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "2",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "partition",
    DataSourceWriteOptions.TABLE_TYPE.key -> DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL,
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    HoodieWriteConfig.RECORD_MERGE_MODE.key -> RecordMergeMode.COMMIT_TIME_ORDERING.name,
    HoodieWriteConfig.TBL_NAME.key -> "base_file_read_per_scan")

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    initHoodieStorage()
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupResources()
    spark = null
  }

  /**
   * Every base file of a scan is read with the same parquet read conf: no conf copy per file for its footer or its
   * reader.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testBaseFilesOfAScanShareOneReadConf(vectorized: Boolean): Unit = {
    writeTable()
    spark.conf.set("spark.sql.parquet.enableVectorizedReader", vectorized.toString)
    val scan = fileScan(spark.read.format("hudi").load(basePath).select("id", "name", "partition"))
    val files = scanFiles(scan)
    assertTrue(files.size >= 3, s"Expected a base file per partition, got $files")

    val hadoopConf = readHadoopConf(scan)
    hadoopConf.set("fs.file.impl", classOf[ReadConfRecordingFileSystem].getName)
    hadoopConf.set("fs.file.impl.disable.cache", "true")
    recordReadConfs.set(true)
    val rows = try {
      readInTasks(spark, readFunction(scan, scan.requiredSchema, hadoopConf), files)
    } finally {
      recordReadConfs.set(false)
    }

    assertEquals(30, rows)
    val confs = dataFileReadConfs(basePath)
    // Spark 4.1+ vectorized reads reuse the footer's stream for the data, so a file may be opened only once.
    assertTrue(confs.size >= files.size, s"Expected at least one open per file, got ${confs.size} for ${files.size} files")
    assertEquals(1, confs.map(System.identityHashCode).distinct.size,
      "All base files of the scan are read with one conf")
  }

  /**
   * The table schema is converted to parquet for logical timestamp repair only when the table has a timestamp-millis
   * field. The conversion builds a new Hadoop conf, which loads the default resources.
   */
  @Test
  def testTableWithoutTimestampMillisSkipsRepairSchemaConversion(): Unit = {
    writeTable()
    val scan = fileScan(spark.read.format("hudi").load(basePath).select("id", "name", "partition"))
    val files = scanFiles(scan)
    val read = readFunction(scan, scan.requiredSchema, readHadoopConf(scan))

    val loads = countDefaultResourceLoads(spark, read, files)

    assertEquals(0, loads, "Reading base files builds no Hadoop conf from the default resources")
  }

  private def writeTable(): Unit = {
    val rows = (0 until 30).map(i => (i.toString, s"name_$i", i.toLong, s"p${i % 3}"))
    spark.createDataFrame(rows).toDF("id", "name", "ts", "partition")
      .write.format("hudi").options(writeOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite).save(basePath)
  }

  private def scanFiles(scan: FileSourceScanExec): Seq[PartitionedFile] =
    scan.inputRDD.asInstanceOf[FileScanRDD].filePartitions.flatMap(_.files)

  private def readHadoopConf(scan: FileSourceScanExec): Configuration =
    spark.sessionState.newHadoopConfWithOptions(scan.relation.options)

  private def readFunction(scan: FileSourceScanExec, requiredSchema: StructType,
                           hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val relation = scan.relation
    relation.fileFormat.buildReaderWithPartitionValues(spark, relation.dataSchema, relation.partitionSchema,
      requiredSchema, Seq.empty, relation.options + (FileFormat.OPTION_RETURNING_BATCH -> "false"), hadoopConf)
  }
}

object TestBaseFileReadPerScan extends AdaptiveSparkPlanHelper {

  /** Whether [[ReadConfRecordingFileSystem]] records the confs files are opened with. */
  val recordReadConfs = new java.util.concurrent.atomic.AtomicBoolean(false)
  private val openedFiles = Collections.synchronizedList(new java.util.ArrayList[(String, Configuration)]())
  private val defaultResourceLoads = new AtomicInteger()

  def recordOpen(path: Path, conf: Configuration): Unit = {
    if (recordReadConfs.get()) {
      openedFiles.add((path.toUri.getPath, conf))
    }
  }

  /** The confs data files under `basePath` were opened with, one per open. */
  def dataFileReadConfs(basePath: String): Seq[Configuration] = {
    val base = new Path(basePath).toUri.getPath
    val opened = openedFiles.toArray(new Array[(String, Configuration)](0)).toSeq
    openedFiles.clear()
    opened.filter { case (path, _) => path.startsWith(base) && path.endsWith(".parquet") && !path.contains("/.hoodie/") }
      .map(_._2)
  }

  def fileScan(df: DataFrame): FileSourceScanExec = {
    val scans = collect(df.queryExecution.executedPlan) { case s: FileSourceScanExec => s }
    assertEquals(1, scans.size, s"Expected one file scan in ${df.queryExecution.executedPlan}")
    scans.head
  }

  /** Runs the read function in Spark tasks, one per file, and returns the number of rows read. */
  def readInTasks(spark: SparkSession, read: PartitionedFile => Iterator[InternalRow], files: Seq[PartitionedFile]): Long =
    spark.sparkContext.parallelize(files, files.size).mapPartitions(_.map(file => read(file).size.toLong)).sum().toLong

  /**
   * Runs the read function in Spark tasks and counts the Hadoop default resources loaded while it runs. A Hadoop conf
   * loads them through the context class loader it was created with, and a copy keeps the loader of its source, so this
   * counts only the confs built from scratch.
   */
  def countDefaultResourceLoads(spark: SparkSession, read: PartitionedFile => Iterator[InternalRow],
                                files: Seq[PartitionedFile]): Int = {
    defaultResourceLoads.set(0)
    spark.sparkContext.parallelize(files, files.size).mapPartitions { it =>
      val thread = Thread.currentThread()
      val original = thread.getContextClassLoader
      thread.setContextClassLoader(new DefaultResourceCountingClassLoader(original))
      try {
        Iterator(it.map(file => read(file).size.toLong).sum)
      } finally {
        thread.setContextClassLoader(original)
      }
    }.collect()
    defaultResourceLoads.get()
  }

  private class DefaultResourceCountingClassLoader(parent: ClassLoader) extends ClassLoader(parent) {
    override def getResource(name: String): URL = {
      if (name == "core-default.xml" || name == "core-site.xml") {
        defaultResourceLoads.incrementAndGet()
      }
      super.getResource(name)
    }
  }
}

/**
 * A local file system that records the conf each file is opened with. Registered with the file system cache off, so
 * every open resolves a new instance initialized with the reader's conf.
 */
class ReadConfRecordingFileSystem extends LocalFileSystem {
  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    TestBaseFileReadPerScan.recordOpen(path, getConf)
    super.open(path, bufferSize)
  }
}
