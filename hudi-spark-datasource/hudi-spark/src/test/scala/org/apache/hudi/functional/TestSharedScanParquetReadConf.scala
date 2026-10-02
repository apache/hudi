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

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.storage.{StorageConfiguration, StoragePath}
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.{FileFormat, SparkColumnarFileReader}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadConf, SharedScanStorageConfiguration}
import org.apache.spark.sql.sources.{Filter, GreaterThan}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNull, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.File
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

import scala.collection.JavaConverters._

/**
 * Base files read with a conf the reader prepares once per scan return the same rows as base files read with a conf
 * prepared per file: with and without the vectorized reader, pushed filters, a file whose column type differs from the
 * requested type, two requested schemas read alternately, and concurrent reads.
 */
class TestSharedScanParquetReadConf extends HoodieSparkClientTestBase {

  var spark: SparkSession = _

  @BeforeEach override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    spark.conf.set("spark.sql.parquet.enableNestedColumnVectorizedReader", "true")
  }

  @AfterEach override def tearDown(): Unit = {
    cleanupResources()
    spark = null
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testSharedReadConfReadsTheSameRowsAsPerFileReadConfs(vectorized: Boolean): Unit = {
    val (dir, files, wideSchema) = writeFiles()
    val narrowSchema = new StructType().add(wideSchema("id")).add(wideSchema("s"))
    val hadoopConf = recordingHadoopConf()
    val reader = sparkAdapter.createParquetFileReader(vectorized, spark.sessionState.conf,
      Map(FileFormat.OPTION_RETURNING_BATCH -> "false"), hadoopConf)
    val sharedConf = new SharedScanStorageConfiguration(hadoopConf)
    val sourceEntries = entries(hadoopConf)

    val rowsByFilters = for (filters <- Seq(Seq.empty[Filter], Seq[Filter](GreaterThan("id", 12L)))) yield {
      val rows = for (file <- files; schema <- Seq(wideSchema, narrowSchema)) yield {
        val perFile = read(reader, file, schema, filters, new HadoopStorageConfiguration(hadoopConf))
        val shared = read(reader, file, schema, filters, sharedConf)
        assertEquals(perFile, shared, s"Rows of ${file.getName} for $schema with filters $filters")
        shared.size
      }
      rows.sum
    }
    assertEquals(2 * 40, rowsByFilters.head)
    assertTrue(rowsByFilters(1) < rowsByFilters.head, "The pushed filter skips row groups")
    assertEquals(sourceEntries, entries(hadoopConf), "Reads do not modify the source conf")

    // Filtered reads set the filter on a copy: the shared conf the next unfiltered read uses has none.
    TestBaseFileReadPerScan.recordReadConfs.set(true)
    try {
      files.foreach(file => read(reader, file, wideSchema, Seq.empty, sharedConf))
    } finally {
      TestBaseFileReadPerScan.recordReadConfs.set(false)
    }
    val confs = TestBaseFileReadPerScan.dataFileReadConfs(dir)
    assertTrue(confs.nonEmpty)
    // The int file reads with its own requested schema, so it has a copy of the shared conf.
    assertTrue(confs.map(System.identityHashCode).distinct.size <= 2, s"${confs.size} opens")
    confs.foreach(conf => assertNull(conf.get(ParquetInputFormat.FILTER_PREDICATE)))
  }

  /**
   * The tasks of an executor read concurrently through one reader and one shared conf: files with and without a pushed
   * filter, and a file with an implicit type change. Every read returns the rows of a per-file read, and the shared
   * read conf is left unchanged.
   */
  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testConcurrentReadsLeaveTheSharedReadConfUnchanged(vectorized: Boolean): Unit = {
    val (dir, files, schema) = writeFiles()
    val hadoopConf = recordingHadoopConf()
    val reader = sparkAdapter.createParquetFileReader(vectorized, spark.sessionState.conf,
      Map(FileFormat.OPTION_RETURNING_BATCH -> "false"), hadoopConf)
    val sharedConf = new SharedScanStorageConfiguration(hadoopConf)
    val reads = for (file <- files; filters <- Seq(Seq.empty[Filter], Seq[Filter](GreaterThan("id", 12L)))) yield (file, filters)
    val expected = reads.map { case (file, filters) =>
      read(reader, file, schema, filters, new HadoopStorageConfiguration(hadoopConf)) }

    // The first read through the shared conf prepares the read conf that all later reads share.
    TestBaseFileReadPerScan.recordReadConfs.set(true)
    try {
      read(reader, files.head, schema, Seq.empty, sharedConf)
    } finally {
      TestBaseFileReadPerScan.recordReadConfs.set(false)
    }
    val readConfs = TestBaseFileReadPerScan.dataFileReadConfs(dir).collect { case conf: ParquetReadConf => conf }
    assertEquals(1, readConfs.map(System.identityHashCode).distinct.size)
    val sharedReadConf = readConfs.head
    val sharedEntries = entries(sharedReadConf)

    val threads = 8
    val rounds = 5
    val pool = Executors.newFixedThreadPool(threads)
    try {
      val start = new CountDownLatch(1)
      val results = (0 until threads).map { thread =>
        pool.submit(new Callable[Seq[Seq[UnsafeRow]]] {
          override def call(): Seq[Seq[UnsafeRow]] = {
            start.await()
            // Each thread starts at a different read, so different kinds of reads overlap.
            (0 until rounds * reads.size).map { i =>
              val (file, filters) = reads((i + thread) % reads.size)
              read(reader, file, schema, filters, sharedConf)
            }
          }
        })
      }
      start.countDown()
      results.zipWithIndex.foreach { case (result, thread) =>
        val rows = result.get(5, TimeUnit.MINUTES)
        rows.zipWithIndex.foreach { case (fileRows, i) =>
          assertEquals(expected((i + thread) % reads.size), fileRows, s"Thread $thread read $i")
        }
      }
    } finally {
      pool.shutdownNow()
    }
    assertEquals(sharedEntries, entries(sharedReadConf), "Concurrent reads leave the shared read conf unchanged")
    assertNull(sharedReadConf.get(ParquetInputFormat.FILTER_PREDICATE))
  }

  /** Three files that store the id as a long and one that stores it as an int, and the schema of the long files. */
  private def writeFiles(): (String, Seq[File], StructType) = {
    val dir = tempDir.resolve("parquet").toAbsolutePath.toString
    spark.range(0, 30, 1, 3)
      .selectExpr("id", "concat('name_', id) as name", "named_struct('a', cast(id as int), 'b', cast(id as string)) as s",
        "array(id, id + 1) as arr")
      .write.parquet(dir + "/long")
    // A file that stores the id as an int: the long request is an implicit type change for it alone.
    spark.range(30, 40, 1, 1)
      .selectExpr("cast(id as int) as id", "concat('name_', id) as name",
        "named_struct('a', cast(id as int), 'b', cast(id as string)) as s", "array(id, id + 1) as arr")
      .write.parquet(dir + "/int")
    val files = Seq("long", "int").flatMap(sub => new File(dir, sub).listFiles().toSeq)
      .filter(_.getName.endsWith(".parquet")).sortBy(_.getName)
    assertEquals(4, files.size)
    (dir, files, spark.read.parquet(dir + "/long").schema)
  }

  /** A Hadoop conf whose local file opens record the conf they use (see [[ReadConfRecordingFileSystem]]). */
  private def recordingHadoopConf(): Configuration = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    hadoopConf.set("fs.file.impl", classOf[ReadConfRecordingFileSystem].getName)
    hadoopConf.set("fs.file.impl.disable.cache", "true")
    hadoopConf
  }

  private def read(reader: SparkColumnarFileReader, file: File, schema: StructType, filters: Seq[Filter],
                   storageConf: StorageConfiguration[Configuration]): Seq[UnsafeRow] = {
    val partitionedFile = sparkAdapter.getSparkPartitionedFileUtils
      .createPartitionedFile(InternalRow.empty, new StoragePath(file.getAbsolutePath), 0, file.length())
    val projection = UnsafeProjection.create(schema)
    val iterator = reader.read(partitionedFile, schema, new StructType(), HOption.empty(), filters, storageConf, HOption.empty())
    try {
      iterator.map(row => projection(row).copy()).toList
    } finally {
      iterator match {
        case closeable: AutoCloseable => closeable.close()
        case _ =>
      }
    }
  }

  private def entries(conf: Configuration): Map[String, String] =
    conf.iterator().asScala.map(e => e.getKey -> e.getValue).toMap
}
