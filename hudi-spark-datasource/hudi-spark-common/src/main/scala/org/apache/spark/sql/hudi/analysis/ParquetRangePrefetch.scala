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
import org.apache.parquet.HadoopReadOptions
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.ParquetMetadata
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
  val MAX_RANGE_BYTES = "hoodie.vector.exact.fetch.max.range.bytes"

  private val DEFAULT_CONCURRENCY = 64
  private val DEFAULT_TIMEOUT_MS = 30000L
  private val DEFAULT_EXPECTED_PAGE_BYTES = 2L * 1024L * 1024L
  private val DEFAULT_ALLOCATION_SLACK = 2.0d
  private val DEFAULT_HEAP_FRACTION = 0.25d
  private val DEFAULT_MAX_RANGE_BYTES = 8L * 1024L * 1024L
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
      maxPlannedBytes: Long,
      readerOpenMs: Long,
      elapsedMs: Long)

  def enabled(conf: Configuration): Boolean = conf.getBoolean(ENABLED, false)

  def prefetch(
      conf: Configuration,
      path: Path,
      fileLength: Long,
      metadata: ParquetMetadata,
      ranges: Seq[PlannedRange],
      candidateCount: Int,
      projectedColumnCount: Int): Session = {
    val prefetchStartNs = System.nanoTime()
    val metadataTail = metadataTailRange(metadata, fileLength)
    val logicalRanges = ranges :+ metadataTail
    val maxRangeBytes = conf.getLong(MAX_RANGE_BYTES, DEFAULT_MAX_RANGE_BYTES)
    require(maxRangeBytes > 0L && maxRangeBytes <= Int.MaxValue,
      s"$MAX_RANGE_BYTES must be in (0, ${Int.MaxValue}]")
    val storagePath = new StoragePath(path.toString)
    val storage = HoodieStorageUtils.getStorage(storagePath, HadoopFSUtils.getStorageConf(conf))
    val requestedConcurrency = conf.getInt(MAX_CONCURRENCY, DEFAULT_CONCURRENCY)
    // GCS connector 2.2.x does not expose a reliable connection-pool property. This explicit cap is
    // operator-declared and bounds Hudi below the connector rather than pretending to configure it.
    val transportLimit = conf.getInt(TRANSPORT_MAX_CONNECTIONS, requestedConcurrency)
    val targetRangeCount = math.max(1, math.min(requestedConcurrency, transportLimit))
    val physicalRanges = splitRanges(
      mergeRanges(logicalRanges), maxRangeBytes, targetRangeCount)
    val timeoutMs = conf.getLong(TIMEOUT_MS, DEFAULT_TIMEOUT_MS)
    val maxRetries = conf.getInt(MAX_RETRIES, 2)
    val maxPlannedBytes = allocationBudget(conf, candidateCount, projectedColumnCount)
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
    val byteRanges = physicalRanges.map(range =>
      new HoodieRangeReadHandle.ByteRange(range.start, Math.toIntExact(range.length))).asJava

    val batch = new BoundedRangeReadHandle(
      storage, requestedConcurrency, transportLimit, maxRetries)
      .readRanges(storagePath, byteRanges, deadline, maxPlannedBytes)
    val regions = logicalRanges.map(range =>
      new ReadRegion(range.start, range.length, range.kind)).asJava
    val inputFile = new HoodiePrefetchedParquetInputFile(
      storage, storagePath, fileLength, batch.getResults, regions)
    val options = HadoopReadOptions.builder(conf, path).build()
    val readerOpenStartNs = System.nanoTime()
    // The metadata tail is prefetched, so this portable path rereads and reparses the footer from
    // memory on every supported Parquet version instead of depending on a version-specific constructor.
    val reader = ParquetFileReader.open(inputFile, options)
    val readerOpenMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - readerOpenStartNs)
    val plannedBytes = physicalRanges.iterator.map(_.length).sum
    val metrics = batch.getMetrics
    val prefetchElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - prefetchStartNs)
    LOG.info(
      s"[vector_search][prefetch] path=$path candidates=$candidateCount plannedRanges=${physicalRanges.size} " +
        s"plannedBytes=$plannedBytes maxPlannedBytes=$maxPlannedBytes maxRangeBytes=$maxRangeBytes " +
        s"metadataTailBytes=${metadataTail.length} requestedConcurrency=$requestedConcurrency " +
        s"transportMaxConnections=$transportLimit effectiveConcurrency=${metrics.getEffectiveConcurrency} " +
        s"fetchWaitMs=${TimeUnit.NANOSECONDS.toMillis(metrics.getWaitNanos)} " +
        s"rangeOpenMs=${TimeUnit.NANOSECONDS.toMillis(metrics.getOpenNanos)} " +
        s"rangeReadMs=${TimeUnit.NANOSECONDS.toMillis(metrics.getReadNanos)} " +
        s"readerOpenMs=$readerOpenMs prefetchElapsedMs=$prefetchElapsedMs " +
        s"readerPath=portable-input-file")
    Session(reader, inputFile, metrics, physicalRanges.size, plannedBytes, maxPlannedBytes,
      readerOpenMs, prefetchElapsedMs)
  }

  private def allocationBudget(
      conf: Configuration,
      candidateCount: Int,
      projectedColumnCount: Int): Long = {
    val expectedPageBytes = conf.getLong(EXPECTED_PAGE_BYTES, DEFAULT_EXPECTED_PAGE_BYTES)
    val slack = conf.getDouble(ALLOCATION_SLACK, DEFAULT_ALLOCATION_SLACK)
    val heapFraction = conf.getDouble(HEAP_FRACTION, DEFAULT_HEAP_FRACTION)
    val defaultActiveTasks = math.max(1, conf.getInt("spark.executor.cores", 1))
    val activeTasks = math.max(1, conf.getInt(ACTIVE_TASKS_PER_EXECUTOR, defaultActiveTasks))
    require(expectedPageBytes > 0L, s"$EXPECTED_PAGE_BYTES must be positive")
    require(slack >= 1.0d, s"$ALLOCATION_SLACK must be at least 1.0")
    require(heapFraction > 0.0d && heapFraction <= 1.0d, s"$HEAP_FRACTION must be in (0, 1]")

    val candidateDerived = BigDecimal(math.max(1, candidateCount)) *
      BigDecimal(math.max(1, projectedColumnCount)) * BigDecimal(expectedPageBytes) * BigDecimal(slack)
    val heapDerived = BigDecimal(Runtime.getRuntime.maxMemory()) * BigDecimal(heapFraction) / BigDecimal(activeTasks)
    candidateDerived.min(heapDerived).min(BigDecimal(Long.MaxValue)).toLong
  }

  /**
   * Starts at the first byte after the last column chunk. Parquet places column/offset indexes and
   * the serialized footer after column data, so this one tail range makes the portable reader open
   * path storage-free. Empty files conservatively prefetch the whole file.
   */
  private[analysis] def metadataTailRange(
      metadata: ParquetMetadata,
      fileLength: Long): PlannedRange = {
    require(fileLength > 0L, s"Parquet file length must be positive: $fileLength")
    val columnEnds = metadata.getBlocks.asScala.iterator
      .flatMap(_.getColumns.asScala.iterator)
      .map { column =>
        val startingPos: Long = column.getStartingPos
        val totalSize: Long = column.getTotalSize
        Math.addExact(startingPos, totalSize)
      }
      .toSeq
    val tailStart = columnEnds.reduceOption((left, right) => math.max(left, right)).getOrElse(0L)
    require(tailStart >= 0L && tailStart < fileLength,
      s"Invalid Parquet metadata tail [$tailStart, $fileLength)")
    PlannedRange(tailStart, fileLength, RegionKind.METADATA)
  }

  private[analysis] def splitRanges(
      ranges: Seq[PlannedRange],
      maxRangeBytes: Long,
      targetRangeCount: Int = 1): Seq[PlannedRange] = {
    require(maxRangeBytes > 0L && maxRangeBytes <= Int.MaxValue,
      s"maxRangeBytes must be in (0, ${Int.MaxValue}]")
    require(targetRangeCount > 0, "targetRangeCount must be positive")
    val totalBytes = ranges.iterator.map(range => BigInt(range.length)).sum
    val targetBytes = if (totalBytes == 0 || ranges.size >= targetRangeCount) {
      maxRangeBytes
    } else {
      ((totalBytes + targetRangeCount - 1) / targetRangeCount).min(BigInt(maxRangeBytes)).toLong
    }
    val chunkBytes = math.max(1L, targetBytes)
    ranges.flatMap { range =>
      Iterator.iterate(range.start)(_ + chunkBytes)
        .takeWhile(_ < range.end)
        .map(start => PlannedRange(start, math.min(range.end, start + chunkBytes), range.kind))
        .toSeq
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
          // Preserve detailed regions separately for miss diagnostics. A merged range's kind is not
          // semantically meaningful; PAGE is only a placeholder for the physical read plan.
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
