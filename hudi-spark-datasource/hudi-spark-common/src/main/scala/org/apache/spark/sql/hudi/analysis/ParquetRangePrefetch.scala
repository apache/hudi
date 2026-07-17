/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.common.util.HoodieStorageUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.io.storage.HoodiePrefetchedParquetInputFile
import org.apache.hudi.io.storage.HoodiePrefetchedParquetInputFile.{ReadRegion, RegionKind}
import org.apache.hudi.storage.{BoundedRangeReadHandle, HoodieRangeReadHandle, StoragePath}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.ParquetReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.SeekableInputStream
import org.slf4j.LoggerFactory

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._

/** Builds a bounded prefetch session without exposing Hadoop or object-store streams to callers. */
private[analysis] object ParquetRangePrefetch {
  private val LOG = LoggerFactory.getLogger(getClass)

  val ENABLED = "hoodie.vector.exact.fetch.prefetch.enabled"
  val MAX_CONCURRENCY = "hoodie.vector.exact.fetch.max.concurrency"
  val TRANSPORT_MAX_CONNECTIONS = "hoodie.vector.exact.fetch.transport.max.connections"
  val TIMEOUT_MS = "hoodie.vector.exact.fetch.timeout.ms"
  val MAX_RETRIES = "hoodie.vector.exact.fetch.max.retries"
  val EXPECTED_PAGE_BYTES = "hoodie.vector.exact.fetch.expected.page.bytes"
  val ALLOCATION_SLACK = "hoodie.vector.exact.fetch.allocation.slack"
  val HEAP_FRACTION = "hoodie.vector.exact.fetch.heap.fraction"
  val ACTIVE_TASKS_PER_EXECUTOR = "hoodie.vector.exact.fetch.active.tasks.per.executor"

  private val DEFAULT_CONCURRENCY = 64
  private val DEFAULT_TIMEOUT_MS = 30000L
  private val DEFAULT_EXPECTED_PAGE_BYTES = 2L * 1024L * 1024L
  private val DEFAULT_ALLOCATION_SLACK = 2.0d
  private val DEFAULT_HEAP_FRACTION = 0.5d
  private val RANGE_MERGE_GAP_BYTES = 512L * 1024L

  final case class PlannedRange(start: Long, end: Long, kind: RegionKind) {
    require(start >= 0L && end > start, s"Invalid planned range [$start, $end)")

    def length: Long = end - start
  }

  final case class Session(
      reader: ParquetFileReader,
      inputFile: HoodiePrefetchedParquetInputFile,
      rangeMetrics: HoodieRangeReadHandle.RangeReadMetrics,
      plannedRanges: Int,
      plannedBytes: Long,
      maxPlannedBytes: Long)

  def enabled(conf: Configuration): Boolean = conf.getBoolean(ENABLED, false)

  def prefetch(
      conf: Configuration,
      path: Path,
      fileLength: Long,
      metadata: ParquetMetadata,
      ranges: Seq[PlannedRange],
      candidateCount: Int,
      projectedColumnCount: Int): Session = {
    val merged = mergeRanges(ranges)
    val storagePath = new StoragePath(path.toString)
    val storage = HoodieStorageUtils.getStorage(storagePath, HadoopFSUtils.getStorageConf(conf))
    val requestedConcurrency = conf.getInt(MAX_CONCURRENCY, DEFAULT_CONCURRENCY)
    // GCS connector 2.2.x does not expose a reliable connection-pool property. This explicit cap is
    // operator-declared and bounds Hudi below the connector rather than pretending to configure it.
    val transportLimit = conf.getInt(TRANSPORT_MAX_CONNECTIONS, requestedConcurrency)
    val timeoutMs = conf.getLong(TIMEOUT_MS, DEFAULT_TIMEOUT_MS)
    val maxRetries = conf.getInt(MAX_RETRIES, 2)
    val maxPlannedBytes = allocationBudget(conf, candidateCount, projectedColumnCount)
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    val byteRanges = merged.map(range =>
      new HoodieRangeReadHandle.ByteRange(range.start, Math.toIntExact(range.length))).asJava

    val batch = new BoundedRangeReadHandle(
      storage, requestedConcurrency, transportLimit, maxRetries)
      .readRanges(storagePath, byteRanges, deadline, maxPlannedBytes)
    val regions = ranges.map(range =>
      new ReadRegion(range.start, range.length, range.kind)).asJava
    val inputFile = new HoodiePrefetchedParquetInputFile(
      storage, storagePath, fileLength, batch.getResults, regions)
    val options = ParquetReadOptions.builder().build()
    val reader = openWithKnownMetadata(conf, path, metadata, options, inputFile)
    val plannedBytes = merged.iterator.map(_.length).sum
    LOG.info(
      s"[vector_search][prefetch] path=$path candidates=$candidateCount plannedRanges=${merged.size} " +
        s"plannedBytes=$plannedBytes maxPlannedBytes=$maxPlannedBytes requestedConcurrency=$requestedConcurrency " +
        s"transportMaxConnections=$transportLimit effectiveConcurrency=${batch.getMetrics.getEffectiveConcurrency}")
    Session(reader, inputFile, batch.getMetrics, merged.size, plannedBytes, maxPlannedBytes)
  }

  private def allocationBudget(
      conf: Configuration,
      candidateCount: Int,
      projectedColumnCount: Int): Long = {
    val expectedPageBytes = conf.getLong(EXPECTED_PAGE_BYTES, DEFAULT_EXPECTED_PAGE_BYTES)
    val slack = conf.getDouble(ALLOCATION_SLACK, DEFAULT_ALLOCATION_SLACK)
    val heapFraction = conf.getDouble(HEAP_FRACTION, DEFAULT_HEAP_FRACTION)
    val activeTasks = math.max(1, conf.getInt(ACTIVE_TASKS_PER_EXECUTOR, 1))
    require(expectedPageBytes > 0L, s"$EXPECTED_PAGE_BYTES must be positive")
    require(slack >= 1.0d, s"$ALLOCATION_SLACK must be at least 1.0")
    require(heapFraction > 0.0d && heapFraction <= 1.0d, s"$HEAP_FRACTION must be in (0, 1]")

    val candidateDerived = BigDecimal(math.max(1, candidateCount)) *
      BigDecimal(math.max(1, projectedColumnCount)) * BigDecimal(expectedPageBytes) * BigDecimal(slack)
    val heapDerived = BigDecimal(Runtime.getRuntime.maxMemory()) * BigDecimal(heapFraction) / BigDecimal(activeTasks)
    candidateDerived.min(heapDerived).min(BigDecimal(Long.MaxValue)).toLong
  }

  /**
   * Runtime Parquet 1.15 can reuse known metadata and our stream directly. The reflection keeps
   * Hudi's Parquet 1.13 compile/runtime compatibility; older Parquet rereads the footer through the
   * containment adapter and records that metadata miss explicitly.
   */
  private def openWithKnownMetadata(
      conf: Configuration,
      path: Path,
      metadata: ParquetMetadata,
      options: ParquetReadOptions,
      inputFile: HoodiePrefetchedParquetInputFile): ParquetFileReader = {
    val stream = inputFile.newStream()
    try {
      val constructor = classOf[ParquetFileReader].getConstructor(
        classOf[Configuration],
        classOf[Path],
        classOf[ParquetMetadata],
        classOf[ParquetReadOptions],
        classOf[SeekableInputStream])
      constructor.newInstance(conf, path, metadata, options, stream)
    } catch {
      case _: NoSuchMethodException =>
        stream.close()
        ParquetFileReader.open(inputFile, options)
      case failure: ReflectiveOperationException =>
        stream.close()
        throw new IllegalStateException(s"Unable to open prefetched Parquet reader for $path", failure)
    }
  }

  private[analysis] def mergeRanges(ranges: Seq[PlannedRange]): Seq[PlannedRange] = {
    val sorted = ranges.sortBy(range => (range.start, range.end))
    if (sorted.isEmpty) {
      Seq.empty
    } else {
      val merged = scala.collection.mutable.ArrayBuffer.empty[PlannedRange]
      var current = sorted.head
      sorted.tail.foreach { next =>
        if (next.start <= current.end || next.start - current.end <= RANGE_MERGE_GAP_BYTES) {
          // Preserve detailed regions separately for miss diagnostics. Physical reads follow the
          // same coalescing policy as the serial baseline so A/B range counts remain comparable.
          current = PlannedRange(current.start, math.max(current.end, next.end), RegionKind.PAGE)
        } else {
          merged += current
          current = next
        }
      }
      merged += current
      merged.toSeq
    }
  }
}
