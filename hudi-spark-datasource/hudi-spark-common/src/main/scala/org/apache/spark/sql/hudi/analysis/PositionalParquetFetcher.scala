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

package org.apache.spark.sql.hudi.analysis

import org.apache.hudi.SparkAdapterSupport
import org.apache.hudi.common.index.vector.{ScoredVectorPostingMatch, VectorIndexMdtSearchUtils, VectorPostingMatch}
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.io.storage.VectorConversionUtils

import com.github.benmanes.caffeine.cache.{Cache, Caffeine}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.api.InitContext
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.internal.column.columnindex.OffsetIndex
import org.apache.parquet.internal.filter2.columnindex.RowRanges
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.MessageType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction.DistanceMetric
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetReaderBase
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import java.util.PrimitiveIterator

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Fetches exact vector-search rerank candidates from Parquet by physical row position.
 *
 * The fetcher uses Parquet page indexes to turn candidate row positions into row ranges, so
 * the reader can skip non-candidate pages instead of scanning whole Hudi file slices.
 */
private[analysis] final class PositionalParquetFetcher(
    conf: Configuration,
    embeddingCol: String,
    vectorSchema: HoodieSchema.Vector,
    queryVector: Array[Double],
    metric: DistanceMetric.Value,
    outputSchema: StructType,
    failOnMissingOffsetIndex: Boolean,
    vectorOnlyProjection: Boolean,
    verifyKeys: Boolean) extends Serializable {

  import PositionalParquetFetcher._

  private val distanceOrdinal = outputSchema.fieldIndex(HoodieVectorSearchPlanBuilder.DISTANCE_COL)
  private val recordKeyOrdinal = fieldOrdinal(HoodieRecord.RECORD_KEY_METADATA_FIELD)
  private val partitionPathOrdinal = fieldOrdinal(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
  private val fileGroupIdOrdinal = fieldOrdinal(HoodieVectorSearchPlanBuilder.FILE_GROUP_ID_COL)
  private val scorer = new VectorExactScorer(vectorSchema, queryVector, metric)

  // Catalyst columns decoded per candidate: the output's stored columns, then the record key when
  // it is verified, then the embedding. VECTOR columns are read as binary, as HoodieSparkParquetReader does.
  private val storedFields: Seq[StructField] =
    if (vectorOnlyProjection) Seq.empty else outputSchema.fields.toSeq.filterNot(f => isSyntheticOutputField(f.name))
  private val readsRecordKey = verifyKeys || vectorOnlyProjection
  private val storedVectorColumns: Map[Int, HoodieSchema.Vector] =
    VectorConversionUtils.detectVectorColumnsFromMetadata(StructType(storedFields)).asScala
      .map { case (ordinal, vector) => ordinal.intValue() -> vector }.toMap
  private val readSchema: StructType = VectorConversionUtils.replaceVectorColumnsWithBinary(
    StructType(storedFields ++
      (if (readsRecordKey) Seq(StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, StringType)) else Seq.empty) :+
      StructField(embeddingCol, BinaryType)),
    storedVectorColumns.map { case (ordinal, vector) => Int.box(ordinal) -> vector }.asJava)
  private val embeddingReadOrdinal = readSchema.fieldIndex(embeddingCol)
  private val recordKeyReadOrdinal =
    if (readsRecordKey) Some(readSchema.fieldIndex(HoodieRecord.RECORD_KEY_METADATA_FIELD)) else None
  /** For each output field, the read-schema ordinal that supplies it, or -1 (synthetic / not read). */
  private val outputSources: Array[Int] = outputSchema.fields.map { field =>
    if (isSyntheticOutputField(field.name)) -1 else storedFields.indexWhere(_.name == field.name)
  }

  def fetch(
      filePath: String,
      candidates: Seq[ScoredVectorPostingMatch]): FetchResult = {
    val startNs = System.nanoTime()
    if (candidates.isEmpty) {
      FetchResult(Iterator.empty, Seq.empty, FetchMetrics(elapsedMs = elapsedMs(startNs)))
    } else {
      fetchNonEmpty(filePath, candidates, startNs)
    }
  }

  private def fetchNonEmpty(
      filePath: String,
      candidates: Seq[ScoredVectorPostingMatch],
      startNs: Long): FetchResult = {
    val path = new Path(filePath)
    configureRandomRead(conf)
    val fs = path.getFileSystem(conf)
    val fileLen = fs.getFileStatus(path).getLen
    val fileCacheKey = s"$filePath#$fileLen"
    val stats = Option(FileSystem.getStatistics(path.toUri.getScheme, fs.getClass))
    val physicalBefore = stats.map(_.getBytesRead).getOrElse(0L)

    val footerStartNs = System.nanoTime()
    val footerResult = footer(conf, path, fileCacheKey)
    val footerMs = elapsedMs(footerStartNs)
    val metadata = footerResult.metadata
    val fileSchema = metadata.getFileMetaData.getSchema
    val fileMetadata = metadata.getFileMetaData.getKeyValueMetaData
    val readSupport = SparkAdapterSupport.sparkAdapter.createParquetReadSupport(
      None, true, true, SparkAdapterSupport.sparkAdapter.getRebaseSpec("CORRECTED"), HOption.empty[MessageType]())
    val readContext = readSupport.init(new InitContext(rowReadConf,
      fileMetadata.asScala.map { case (key, value) => key -> java.util.Collections.singleton(value) }.asJava, fileSchema))
    val requestedParquetSchema = readContext.getRequestedSchema

    val rowGroups = metadata.getBlocks.asScala.toIndexedSeq
    val rowGroupStarts = cumulativeRowGroupStarts(rowGroups)
    val byRowGroup: Map[Int, Seq[ScoredVectorPostingMatch]] =
      candidates.groupBy(candidate => rowGroupOrdinal(rowGroupStarts, rowGroups, rowPosition(candidate)))

    val rows = mutable.ArrayBuffer.empty[InternalRow]
    val keyMismatches = mutable.ArrayBuffer.empty[ScoredVectorPostingMatch]
    var rowGroupsSelected = 0
    var pagesInSelectedRowGroups = 0L
    var pagesSelected = 0L
    var offsetIndexHits = 0L
    var offsetIndexMissing = 0L
    var pageBytesFetched = 0L
    var rangedGets = 0L
    var rowsDecoded = 0L
    var rowsMaterialized = 0L
    var nullEmbeddingsSkipped = 0L
    var offsetIndexMs = 0L
    var decodeMs = 0L
    var scoreMs = 0L

    val reader = new ParquetFileReader(conf, path, metadata)
    try {
      reader.setRequestedSchema(requestedParquetSchema)
      byRowGroup.toSeq.sortBy(_._1).foreach { case (rgOrdinal, rgCandidates) =>
        val block = rowGroups(rgOrdinal)
        val rowGroupStart = rowGroupStarts(rgOrdinal)
        val embeddingChunk = findColumn(block, embeddingCol)
          .getOrElse(throw new IllegalArgumentException(s"Parquet file '$filePath' is missing vector column '$embeddingCol'."))

        val offsetStartNs = System.nanoTime()
        val embeddingOffsetIndex = offsetIndex(reader, fileCacheKey, rgOrdinal, embeddingChunk)
        offsetIndexMs += elapsedMs(offsetStartNs)
        val relativeRows = rgCandidates.map(candidate => rowPosition(candidate) - rowGroupStart).sorted
        val candidateRows = relativeRows.toSet
        val candidateByRelativeRow = rgCandidates
          .map(candidate => rowPosition(candidate) - rowGroupStart -> candidate)
          .toMap

        if (embeddingOffsetIndex == null && failOnMissingOffsetIndex) {
          offsetIndexMissing += 1
          throw new IllegalStateException(
            s"Parquet file '$filePath' has no OffsetIndex for vector column '$embeddingCol'; positional exact fetch cannot honor pages.")
        }

        rowGroupsSelected += 1
        val (pageStore, rowRanges) = if (embeddingOffsetIndex == null) {
          offsetIndexMissing += 1
          LOG.warn(
            "Parquet file {} rowGroup={} has no OffsetIndex for vector column {}; falling back to row-group fetch.",
            filePath,
            Int.box(rgOrdinal),
            embeddingCol)
          pagesInSelectedRowGroups += 0L
          pagesSelected += 0L
          rangedGets += 1L
          pageBytesFetched += requestedParquetSchema.getFields.asScala
            .flatMap(field => columnsOf(block, field.getName))
            .map(_.getTotalSize)
            .sum
          (reader.readRowGroup(rgOrdinal), RowRanges.createSingle(block.getRowCount))
        } else {
          offsetIndexHits += 1
          pagesInSelectedRowGroups += embeddingOffsetIndex.getPageCount
          val selectedVectorPages = selectedPages(embeddingOffsetIndex, block.getRowCount, relativeRows)
          pagesSelected += selectedVectorPages.size
          val selectedRanges = requestedParquetSchema.getFields.asScala
            .flatMap(field => columnsOf(block, field.getName))
            .flatMap { chunk =>
              val oiStartNs = System.nanoTime()
              val chunkOffsetIndex = offsetIndex(reader, fileCacheKey, rgOrdinal, chunk)
              offsetIndexMs += elapsedMs(oiStartNs)
              if (chunkOffsetIndex == null) {
                offsetIndexMissing += 1
                Seq.empty
              } else {
                offsetIndexHits += 1
                selectedPages(chunkOffsetIndex, block.getRowCount, relativeRows).map { pageOrdinal =>
                  val start = chunkOffsetIndex.getOffset(pageOrdinal)
                  PageRange(start, start + chunkOffsetIndex.getCompressedPageSize(pageOrdinal))
                }
              }
            }
          val mergedRanges = mergeRanges(selectedRanges.sortBy(_.start).toSeq)
          rangedGets += mergedRanges.size
          pageBytesFetched += mergedRanges.iterator.map(range => range.end - range.start).sum
          val ranges = RowRanges.create(block.getRowCount, primitiveIntIterator(selectedVectorPages), embeddingOffsetIndex)
          (reader.readFilteredRowGroup(rgOrdinal, ranges), ranges)
        }

        val decodeStartNs = System.nanoTime()
        val materializer = readSupport.prepareForRead(rowReadConf, fileMetadata, fileSchema, readContext)
        val recordReader = new ColumnIOFactory().getColumnIO(requestedParquetSchema, fileSchema)
          .getRecordReader(pageStore, materializer)
        val rowIterator = rowRanges.iterator()
        while (rowIterator.hasNext) {
          val relativeRow = rowIterator.nextLong()
          val decoded = recordReader.read()
          rowsDecoded += 1
          candidateByRelativeRow.get(relativeRow).foreach { candidate =>
            // The materializer reuses its row and may share page buffers: copy before retaining.
            val row = decoded.copy()
            if (row.isNullAt(embeddingReadOrdinal)) {
              // The live row has a NULL embedding: brute force filters it out, so skip it too.
              nullEmbeddingsSkipped += 1
            } else if (verifyKeys && decodedRecordKey(row) != candidate.getRecordKey) {
              keyMismatches += candidate
            } else {
              val scoreStartNs = System.nanoTime()
              val distance = scorer.scoreBytes(row.getBinary(embeddingReadOrdinal))
              scoreMs += elapsedMs(scoreStartNs)
              val output = toOutputRow(row, distance)
              fillSyntheticFields(output, candidate)
              rows += output
              rowsMaterialized += 1
            }
          }
        }
        decodeMs += elapsedMs(decodeStartNs)
      }
    } finally {
      reader.close()
    }

    val physicalAfter = stats.map(_.getBytesRead).getOrElse(physicalBefore)
    val metrics = FetchMetrics(
      rerankCandidates = candidates.size,
      rowGroupsTotal = rowGroups.size,
      rowGroupsSelected = rowGroupsSelected,
      pagesInSelectedRowGroups = pagesInSelectedRowGroups,
      pagesSelected = pagesSelected,
      offsetIndexHits = offsetIndexHits,
      offsetIndexMissing = offsetIndexMissing,
      rangedGets = rangedGets,
      pageBytesFetched = pageBytesFetched,
      rowsDecoded = rowsDecoded,
      rowsMaterialized = rowsMaterialized,
      nullEmbeddingsSkipped = nullEmbeddingsSkipped,
      fetchKeyMismatch = keyMismatches.size,
      physicalBytesRead = math.max(0L, physicalAfter - physicalBefore),
      footerReads = if (footerResult.cacheHit) 0 else 1,
      footerCacheHits = if (footerResult.cacheHit) 1 else 0,
      footerMs = footerMs,
      offsetIndexMs = offsetIndexMs,
      fetchWaitMs = 0L,
      decodeMs = decodeMs,
      scoreMs = scoreMs,
      elapsedMs = elapsedMs(startNs))
    FetchResult(rows.iterator, keyMismatches.toSeq, metrics)
  }

  @transient private lazy val rowReadConf: Configuration = {
    val rowConf = new Configuration(conf)
    SparkParquetReaderBase.configureRowRead(rowConf, readSchema)
    rowConf
  }

  /** Projects a decoded candidate onto the output schema, restoring VECTOR columns to arrays. */
  private def toOutputRow(row: InternalRow, distance: Double): GenericInternalRow = {
    val values = new Array[Any](outputSchema.length)
    var ordinal = 0
    while (ordinal < values.length) {
      val source = outputSources(ordinal)
      if (source >= 0 && !row.isNullAt(source)) {
        values(ordinal) = storedVectorColumns.get(source) match {
          case Some(vector) => VectorConversionUtils.convertBinaryToVectorArray(row.getBinary(source), vector)
          case None => row.get(source, readSchema(source).dataType)
        }
      }
      ordinal += 1
    }
    values(distanceOrdinal) = distance
    new GenericInternalRow(values)
  }

  private def isSyntheticOutputField(name: String): Boolean =
    name == HoodieVectorSearchPlanBuilder.DISTANCE_COL ||
      name == HoodieRecord.RECORD_KEY_METADATA_FIELD ||
      name == HoodieRecord.PARTITION_PATH_METADATA_FIELD ||
      name == HoodieVectorSearchPlanBuilder.FILE_GROUP_ID_COL

  private def rowPosition(candidate: ScoredVectorPostingMatch): Long = {
    val location = candidate.getLocation
    if (location == null) {
      throw new IllegalArgumentException(
        s"Vector exact positional fetch requires a row-position locator for record ${candidate.getRecordKey}.")
    }
    location.getPosition
  }

  private def decodedRecordKey(row: InternalRow): String =
    recordKeyReadOrdinal.flatMap(ordinal => Option(row.getUTF8String(ordinal))).map(_.toString).orNull

  private def fillSyntheticFields(row: InternalRow, candidate: ScoredVectorPostingMatch): Unit = {
    val liveLocation = Option(candidate.getLocation)
    recordKeyOrdinal.foreach(row.update(_, UTF8String.fromString(candidate.getRecordKey)))
    partitionPathOrdinal.foreach(row.update(_, UTF8String.fromString(
      liveLocation.map(_.getPartitionPath).orElse(Option(candidate.getPartitionPath)).getOrElse(""))))
    fileGroupIdOrdinal.foreach(row.update(_, UTF8String.fromString(
      liveLocation.map(_.getFileId).orElse(Option(candidate.getFileGroupId)).getOrElse(""))))
  }

  private def fieldOrdinal(name: String): Option[Int] =
    outputSchema.fields.indexWhere(_.name == name) match {
      case -1 => None
      case ordinal => Some(ordinal)
    }
}

private[analysis] object PositionalParquetFetcher {
  private val LOG = LoggerFactory.getLogger(getClass)
  // Executor-lifetime caches keyed by path+length (immutable once written). Bounded so a long-lived
  // executor serving many tables cannot grow them without limit; entries are cheap to rebuild.
  private val FOOTER_CACHE_MAX_ENTRIES = 4096L
  private val OFFSET_INDEX_CACHE_MAX_ENTRIES = 65536L
  private val FOOTER_CACHE: Cache[String, ParquetMetadata] =
    Caffeine.newBuilder().maximumSize(FOOTER_CACHE_MAX_ENTRIES).build[String, ParquetMetadata]()
  private val OFFSET_INDEX_CACHE: Cache[String, Option[OffsetIndex]] =
    Caffeine.newBuilder().maximumSize(OFFSET_INDEX_CACHE_MAX_ENTRIES).build[String, Option[OffsetIndex]]()
  private val RANGE_MERGE_GAP_BYTES = 512L * 1024L

  final case class FetchResult(
      rows: Iterator[InternalRow],
      retryByKey: Seq[ScoredVectorPostingMatch],
      metrics: FetchMetrics)

  final case class FetchMetrics(
      rerankCandidates: Int = 0,
      rowGroupsTotal: Int = 0,
      rowGroupsSelected: Int = 0,
      pagesInSelectedRowGroups: Long = 0L,
      pagesSelected: Long = 0L,
      offsetIndexHits: Long = 0L,
      offsetIndexMissing: Long = 0L,
      rangedGets: Long = 0L,
      pageBytesFetched: Long = 0L,
      rowsDecoded: Long = 0L,
      rowsMaterialized: Long = 0L,
      nullEmbeddingsSkipped: Long = 0L,
      fetchKeyMismatch: Long = 0L,
      physicalBytesRead: Long = 0L,
      footerReads: Long = 0L,
      footerCacheHits: Long = 0L,
      footerMs: Long = 0L,
      offsetIndexMs: Long = 0L,
      fetchWaitMs: Long = 0L,
      decodeMs: Long = 0L,
      scoreMs: Long = 0L,
      elapsedMs: Long = 0L) {

    def add(other: FetchMetrics): FetchMetrics = FetchMetrics(
      rerankCandidates + other.rerankCandidates,
      math.max(rowGroupsTotal, other.rowGroupsTotal),
      rowGroupsSelected + other.rowGroupsSelected,
      pagesInSelectedRowGroups + other.pagesInSelectedRowGroups,
      pagesSelected + other.pagesSelected,
      offsetIndexHits + other.offsetIndexHits,
      offsetIndexMissing + other.offsetIndexMissing,
      rangedGets + other.rangedGets,
      pageBytesFetched + other.pageBytesFetched,
      rowsDecoded + other.rowsDecoded,
      rowsMaterialized + other.rowsMaterialized,
      nullEmbeddingsSkipped + other.nullEmbeddingsSkipped,
      fetchKeyMismatch + other.fetchKeyMismatch,
      physicalBytesRead + other.physicalBytesRead,
      footerReads + other.footerReads,
      footerCacheHits + other.footerCacheHits,
      footerMs + other.footerMs,
      offsetIndexMs + other.offsetIndexMs,
      fetchWaitMs + other.fetchWaitMs,
      decodeMs + other.decodeMs,
      scoreMs + other.scoreMs,
      math.max(elapsedMs, other.elapsedMs))
  }

  private final case class FooterResult(metadata: ParquetMetadata, cacheHit: Boolean)

  private final case class PageRange(start: Long, end: Long)

  private def footer(conf: Configuration, path: Path, cacheKey: String): FooterResult = {
    val cached = FOOTER_CACHE.getIfPresent(cacheKey)
    if (cached != null) {
      FooterResult(cached, cacheHit = true)
    } else {
      val metadata = ParquetFileReader.readFooter(conf, path)
      val prior = FOOTER_CACHE.asMap().putIfAbsent(cacheKey, metadata)
      FooterResult(if (prior == null) metadata else prior, cacheHit = prior != null)
    }
  }

  private def offsetIndex(
      reader: ParquetFileReader,
      fileCacheKey: String,
      rowGroupOrdinal: Int,
      chunk: ColumnChunkMetaData): OffsetIndex = {
    val key = s"$fileCacheKey#$rowGroupOrdinal#${chunk.getPath.toDotString}"
    OFFSET_INDEX_CACHE.get(key, _ => Option(reader.readOffsetIndex(chunk))).orNull
  }

  private def configureRandomRead(conf: Configuration): Unit = {
    conf.setIfUnset("fs.s3a.input.fadvise", "random")
    conf.setIfUnset("fs.s3a.experimental.input.fadvise", "random")
    conf.setIfUnset("fs.s3a.readahead.range", "64K")
    conf.setIfUnset("fs.gs.inputstream.fadvise", "RANDOM")
    conf.setIfUnset("fs.gs.inputstream.inplace.seek.limit", "0")
    conf.setIfUnset("fs.azure.read.optimizeforrandom", "true")
    conf.setIfUnset("fs.azure.readaheadqueue.depth", "0")
    conf.setIfUnset("fs.abfs.inputstream.read.readaheadqueue.depth", "0")
  }

  private def elapsedMs(startNs: Long): Long = (System.nanoTime() - startNs) / 1000000L

  private def cumulativeRowGroupStarts(rowGroups: IndexedSeq[BlockMetaData]): IndexedSeq[Long] = {
    var nextStart = 0L
    rowGroups.map { block =>
      val start = nextStart
      nextStart += block.getRowCount
      start
    }
  }

  private def rowGroupOrdinal(
      rowGroupStarts: IndexedSeq[Long],
      rowGroups: IndexedSeq[BlockMetaData],
      rowPosition: Long): Int =
    rowGroupOrdinal(rowGroupStarts, rowGroups.map(_.getRowCount), rowPosition)

  private[analysis] def rowGroupOrdinal(
      rowGroupStarts: IndexedSeq[Long],
      rowGroupRowCounts: IndexedSeq[Long],
      rowPosition: Long): Int = {
    var low = 0
    var high = rowGroupStarts.length - 1
    var insertion = -1
    while (low <= high) {
      val mid = (low + high) >>> 1
      if (rowGroupStarts(mid) <= rowPosition) {
        insertion = mid
        low = mid + 1
      } else {
        high = mid - 1
      }
    }
    if (insertion < 0 || rowPosition >= rowGroupStarts(insertion) + rowGroupRowCounts(insertion)) {
      throw new IllegalArgumentException(s"Candidate row position $rowPosition is outside Parquet row group bounds.")
    }
    insertion
  }

  private def findColumn(block: BlockMetaData, name: String): Option[ColumnChunkMetaData] =
    block.getColumns.asScala.find(_.getPath.toDotString == name)

  /** Every leaf column chunk of a top-level field; nested fields span several chunks. */
  private def columnsOf(block: BlockMetaData, name: String): Seq[ColumnChunkMetaData] =
    block.getColumns.asScala.filter(_.getPath.toArray.head == name).toSeq

  private def selectedPages(
      offsetIndex: OffsetIndex,
      rowGroupRowCount: Long,
      sortedRows: Seq[Long]): Seq[Int] =
    sortedRows.iterator.map(row => pageOrdinalForRow(offsetIndex, rowGroupRowCount, row)).toSet.toSeq.sorted

  private def pageOrdinalForRow(offsetIndex: OffsetIndex, rowGroupRowCount: Long, row: Long): Int = {
    var low = 0
    var high = offsetIndex.getPageCount - 1
    var pageOrdinal = -1
    while (low <= high) {
      val mid = (low + high) >>> 1
      val first = offsetIndex.getFirstRowIndex(mid)
      val last = offsetIndex.getLastRowIndex(mid, rowGroupRowCount)
      if (row < first) {
        high = mid - 1
      } else if (row > last) {
        low = mid + 1
      } else {
        pageOrdinal = mid
        low = high + 1
      }
    }
    if (pageOrdinal < 0) {
      throw new IllegalArgumentException(
        s"Candidate row $row is outside OffsetIndex page bounds for row group with $rowGroupRowCount rows.")
    }
    pageOrdinal
  }

  private def primitiveIntIterator(values: Seq[Int]): PrimitiveIterator.OfInt = {
    val iterator = values.iterator
    new PrimitiveIterator.OfInt {
      override def hasNext: Boolean = iterator.hasNext

      override def nextInt(): Int = iterator.next()
    }
  }

  private def mergeRanges(ranges: Seq[PageRange]): Seq[PageRange] = {
    if (ranges.isEmpty) {
      Seq.empty
    } else {
      val merged = mutable.ArrayBuffer.empty[PageRange]
      var current = ranges.head
      ranges.tail.foreach { range =>
        if (range.start - current.end <= RANGE_MERGE_GAP_BYTES) {
          current = PageRange(current.start, math.max(current.end, range.end))
        } else {
          merged += current
          current = range
        }
      }
      merged += current
      merged.toSeq
    }
  }
}
