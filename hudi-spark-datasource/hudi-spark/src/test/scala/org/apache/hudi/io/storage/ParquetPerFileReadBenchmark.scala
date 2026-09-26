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

package org.apache.hudi.io.storage

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.common.util.{HoodieStorageUtils, Option => HOption}
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.util.CloseableInternalRowIterator

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.HoodieSparkSchemaConverters
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir

import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

/**
 * Manual benchmark of the per-file CPU cost of the Spark parquet read paths: the log-file reader
 * ([[HoodieSparkParquetReader]]) and the base-file reader built by the Spark adapter. Not run in CI
 * (the class name matches no surefire include). Every round reads the same small files and checks
 * the row count and a checksum, so a faster but wrong reader fails the run.
 *
 * {{{
 * mvn test -pl hudi-spark-datasource/hudi-spark -Dtest=ParquetPerFileReadBenchmark \
 *   -Dbenchmark.files=400 -Dbenchmark.rounds=10 -Dbenchmark.warmupRounds=3
 * }}}
 */
class ParquetPerFileReadBenchmark {

  @TempDir
  var tmpDir: Path = _

  @Test
  def benchmark(): Unit = {
    val numFiles = Integer.getInteger("benchmark.files", 400).intValue()
    val rounds = Integer.getInteger("benchmark.rounds", 10).intValue()
    val warmupRounds = Integer.getInteger("benchmark.warmupRounds", 3).intValue()
    val rowsPerFile = 20
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("ParquetPerFileReadBenchmark")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      val dir = tmpDir.resolve("data").toString
      spark.range(0, numFiles.toLong * rowsPerFile, 1, numFiles)
        .selectExpr("concat('key', cast(id as string)) as key", "id as ts", "concat('value', cast(id as string)) as value")
        .write.parquet(dir)
      val files = Files.list(tmpDir.resolve("data")).iterator().asScala
        .filter(_.getFileName.toString.endsWith(".parquet")).map(_.toString).toSeq.sorted
      assertEquals(numFiles, files.size)

      val hadoopConf = spark.sessionState.newHadoopConf()
      val storageConf = HadoopFSUtils.getStorageConf(hadoopConf)
      val storage = HoodieStorageUtils.getStorage(new StoragePath(files.head), storageConf)
      val structType = spark.read.parquet(files.head).schema
      val schema = HoodieSparkSchemaConverters.toHoodieType(structType, nullable = false, "record", "")
      val baseFileReader = sparkAdapter.createParquetFileReader(vectorized = true, spark.sessionState.conf,
        Map(FileFormat.OPTION_RETURNING_BATCH -> "false"), hadoopConf)
      val expectedChecksum = (0L until numFiles.toLong * rowsPerFile).sum

      def readLogPath(file: String): (Long, Long) = {
        val reader = new HoodieSparkFileReaderFactory(storage).newParquetFileReader(new StoragePath(file))
          .asInstanceOf[HoodieSparkParquetReader]
        try {
          val iterator = reader.getUnsafeRowIterator(schema)
          var rows = 0L
          var checksum = 0L
          while (iterator.hasNext) {
            checksum += iterator.next().getLong(1)
            rows += 1
          }
          (rows, checksum)
        } finally {
          reader.close()
        }
      }

      def readBasePath(file: String): (Long, Long) = {
        val partitionedFile = sparkAdapter.getSparkPartitionedFileUtils
          .createPartitionedFile(InternalRow.empty, new StoragePath(file), 0, Files.size(java.nio.file.Paths.get(file)))
        // Mirrors SparkFileFormatInternalRowReaderContext, which converts the table schema for every base file.
        val tableSchema = getAvroSchemaConverter(storageConf.unwrap()).convert(schema)
        val iterator = new CloseableInternalRowIterator(baseFileReader.read(partitionedFile, structType,
          new StructType(), HOption.empty(), Seq.empty, storageConf, HOption.of(tableSchema)))
        try {
          var rows = 0L
          var checksum = 0L
          while (iterator.hasNext) {
            checksum += iterator.next().getLong(1)
            rows += 1
          }
          (rows, checksum)
        } finally {
          iterator.close()
        }
      }

      // Reference costs of single per-file steps, measured the same way; they read no rows.
      def copyConf(file: String): (Long, Long) = {
        new Configuration(hadoopConf)
        (-1L, -1L)
      }

      def newConfiguration(file: String): (Long, Long) = {
        new Configuration().size()
        (-1L, -1L)
      }

      def convertTableSchema(file: String): (Long, Long) = {
        getAvroSchemaConverter(storageConf.unwrap()).convert(schema)
        (-1L, -1L)
      }

      val threadMx = ManagementFactory.getThreadMXBean
      val paths = Seq("log" -> (readLogPath _), "base" -> (readBasePath _), "confCopy" -> (copyConf _),
        "newConfiguration" -> (newConfiguration _), "tableSchemaConversion" -> (convertTableSchema _))
      val results = paths.map { case (name, read) =>
        val perFileCpuMicros = (0 until warmupRounds + rounds).map { _ =>
          val start = threadMx.getCurrentThreadCpuTime
          var rows = 0L
          var checksum = 0L
          files.foreach { file =>
            val (r, c) = read(file)
            rows += r
            checksum += c
          }
          val elapsed = threadMx.getCurrentThreadCpuTime - start
          if (rows >= 0) {
            assertEquals(numFiles.toLong * rowsPerFile, rows)
            assertEquals(expectedChecksum, checksum)
          }
          elapsed / 1000.0 / numFiles
        }.drop(warmupRounds).sorted
        name -> perFileCpuMicros
      }
      // scalastyle:off println
      println(s"ParquetPerFileReadBenchmark: files=$numFiles rowsPerFile=$rowsPerFile rounds=$rounds " +
        s"warmupRounds=$warmupRounds hadoopConfEntries=${hadoopConf.size()}")
      results.foreach { case (name, micros) =>
        println(f"ParquetPerFileReadBenchmark: path=$name cpuMicrosPerFile median=${micros(micros.size / 2)}%.1f " +
          f"min=${micros.head}%.1f max=${micros.last}%.1f")
      }
      // scalastyle:on println
    } finally {
      spark.stop()
    }
  }
}
