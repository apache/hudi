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

import org.apache.hudi.common.index.vector.VectorIndexMdtSearchUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.schema.HoodieSchema

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.metadata.{BlockMetaData, ColumnChunkMetaData, ParquetMetadata}
import org.apache.parquet.internal.column.columnindex.OffsetIndex
import org.apache.parquet.internal.filter2.columnindex.RowRanges
import org.apache.parquet.io.{ColumnIOFactory, RecordReader}
import org.apache.parquet.io.api.{Binary, Converter, GroupConverter, PrimitiveConverter, RecordMaterializer}
import org.apache.parquet.schema.MessageType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.logical.HoodieVectorSearchTableValuedFunction.DistanceMetric
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.slf4j.LoggerFactory

import java.nio.{ByteBuffer, ByteOrder}
import java.util.PrimitiveIterator
import java.util.concurrent.ConcurrentHashMap

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
    vectorOnlyProjection: Boolean) extends Serializable {

  import PositionalParquetFetcher._

  private val distanceOrdinal = outputSchema.fieldIndex(HoodieVectorSearchPlanBuilder.DISTANCE_COL)
  private val recordKeyOrdinal = fieldOrdinal(HoodieRecord.RECORD_KEY_METADATA_FIELD)
  private val partitionPathOrdinal = fieldOrdinal(HoodieRecord.PARTITION_PATH_METADATA_FIELD)
  private val fileGroupIdOrdinal = fieldOrdinal(HoodieVectorSearchPlanBuilder.FILE_GROUP_ID_COL)
  private val queryNorm = math.sqrt(queryVector.iterator.map(v => v * v).sum)
  private var scoreMs: Long = 0L

  def fetch(
      filePath: String,
      candidates: Seq[VectorIndexMdtSearchUtils.ScoredPostingMatch]): FetchResult = {
    val startNs = System.nanoTime()
    if (candidates.isEmpty) {
      FetchResult(Iterator.empty, FetchMetrics(elapsedMs = elapsedMs(startNs)))
    } else {
      fetchNonEmpty(filePath, candidates, startNs)
    }
  }

  private def fetchNonEmpty(
      filePath: String,
      candidates: Seq[VectorIndexMdtSearchUtils.ScoredPostingMatch],
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
    val requestedParquetSchema = requestedSchema(fileSchema)

    val rowGroups = metadata.getBlocks.asScala.toIndexedSeq
    val rowGroupStarts = cumulativeRowGroupStarts(rowGroups)
    val byRowGroup: Map[Int, Seq[VectorIndexMdtSearchUtils.ScoredPostingMatch]] =
      candidates.groupBy(candidate => rowGroupOrdinal(rowGroupStarts, rowGroups, rowPosition(candidate)))

    val rows = mutable.ArrayBuffer.empty[InternalRow]
    var rowGroupsSelected = 0
    var pagesInSelectedRowGroups = 0L
    var pagesSelected = 0L
    var offsetIndexHits = 0L
    var offsetIndexMissing = 0L
    var pageBytesFetched = 0L
    var rangedGets = 0L
    var rowsDecoded = 0L
    var rowsMaterialized = 0L
    var offsetIndexMs = 0L
    var decodeMs = 0L
    scoreMs = 0L

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
            .flatMap(field => findColumn(block, field.getName))
            .map(_.getTotalSize)
            .sum
          (reader.readRowGroup(rgOrdinal), RowRanges.createSingle(block.getRowCount))
        } else {
          offsetIndexHits += 1
          pagesInSelectedRowGroups += embeddingOffsetIndex.getPageCount
          val selectedVectorPages = selectedPages(embeddingOffsetIndex, block.getRowCount, relativeRows)
          pagesSelected += selectedVectorPages.size
          val selectedRanges = requestedParquetSchema.getFields.asScala
            .flatMap(field => findColumn(block, field.getName))
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
        val recordReader = parquetRecordReader(fileSchema, requestedParquetSchema, pageStore)
        val rowIterator = rowRanges.iterator()
        while (rowIterator.hasNext) {
          val relativeRow = rowIterator.nextLong()
          val row = recordReader.read()
          rowsDecoded += 1
          candidateByRelativeRow.get(relativeRow).foreach { candidate =>
            fillSyntheticFields(row, candidate)
            rows += row
            rowsMaterialized += 1
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
      physicalBytesRead = math.max(0L, physicalAfter - physicalBefore),
      footerReads = if (footerResult.cacheHit) 0 else 1,
      footerCacheHits = if (footerResult.cacheHit) 1 else 0,
      footerMs = footerMs,
      offsetIndexMs = offsetIndexMs,
      fetchWaitMs = 0L,
      decodeMs = decodeMs,
      scoreMs = scoreMs,
      elapsedMs = elapsedMs(startNs))
    FetchResult(rows.iterator, metrics)
  }

  private def parquetRecordReader(
      fileSchema: MessageType,
      requestedParquetSchema: MessageType,
      pageStore: PageReadStore): RecordReader[InternalRow] = {
    val materializer = new InternalRowMaterializer(requestedParquetSchema, outputSchema, embeddingCol, distanceOrdinal)
    new ColumnIOFactory().getColumnIO(requestedParquetSchema, fileSchema)
      .getRecordReader(pageStore, materializer)
  }

  private def requestedSchema(fileSchema: MessageType): MessageType = {
    val parquetFields =
      if (vectorOnlyProjection) {
        mutable.ArrayBuffer.empty[org.apache.parquet.schema.Type]
      } else {
        outputSchema.fields
          .filterNot(field => isSyntheticOutputField(field.name))
          .map(field => fieldByName(fileSchema, field.name))
          .toBuffer[org.apache.parquet.schema.Type]
      }
    if (!parquetFields.exists(_.getName == embeddingCol)) {
      parquetFields += fieldByName(fileSchema, embeddingCol)
    }
    new MessageType(fileSchema.getName, parquetFields.asJava)
  }

  private def isSyntheticOutputField(name: String): Boolean =
    name == HoodieVectorSearchPlanBuilder.DISTANCE_COL ||
      name == HoodieRecord.RECORD_KEY_METADATA_FIELD ||
      name == HoodieRecord.PARTITION_PATH_METADATA_FIELD ||
      name == HoodieVectorSearchPlanBuilder.FILE_GROUP_ID_COL

  private def fieldByName(schema: MessageType, name: String): org.apache.parquet.schema.Type =
    schema.getFields.asScala.find(_.getName == name)
      .getOrElse(throw new IllegalArgumentException(s"Parquet schema is missing requested field '$name'."))

  private def rowPosition(candidate: VectorIndexMdtSearchUtils.ScoredPostingMatch): Long = {
    val location = candidate.getLocation
    if (location == null) {
      throw new IllegalArgumentException(
        s"Vector exact positional fetch requires a row-position locator for record ${candidate.getRecordKey}.")
    }
    location.getPosition
  }

  private def fillSyntheticFields(row: InternalRow, candidate: VectorIndexMdtSearchUtils.ScoredPostingMatch): Unit = {
    recordKeyOrdinal.foreach(row.update(_, UTF8String.fromString(candidate.getRecordKey)))
    partitionPathOrdinal.foreach(row.update(_, UTF8String.fromString(Option(candidate.getPartitionPath).getOrElse(""))))
    fileGroupIdOrdinal.foreach(row.update(_, UTF8String.fromString(Option(candidate.getFileGroupId).getOrElse(""))))
  }

  private def fieldOrdinal(name: String): Option[Int] =
    outputSchema.fields.indexWhere(_.name == name) match {
      case -1 => None
      case ordinal => Some(ordinal)
    }

  private def scoreVector(bytes: Array[Byte]): Double = {
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN)
    vectorSchema.getVectorElementType match {
      case HoodieSchema.Vector.VectorElementType.FLOAT =>
        score(vectorSchema.getDimension, i => buffer.getFloat(i * java.lang.Float.BYTES).toDouble)
      case HoodieSchema.Vector.VectorElementType.DOUBLE =>
        score(vectorSchema.getDimension, i => buffer.getDouble(i * java.lang.Double.BYTES))
      case HoodieSchema.Vector.VectorElementType.INT8 =>
        score(vectorSchema.getDimension, i => buffer.get(i).toDouble)
      case other =>
        throw new UnsupportedOperationException(s"Unsupported vector element type for positional exact fetch: $other")
    }
  }

  private def score(dim: Int, valueAt: Int => Double): Double = {
    metric match {
      case DistanceMetric.L2 =>
        var sum = 0.0d
        var i = 0
        while (i < dim) {
          val diff = valueAt(i) - queryVector(i)
          sum += diff * diff
          i += 1
        }
        math.sqrt(sum)
      case DistanceMetric.COSINE =>
        var dot = 0.0d
        var norm = 0.0d
        var i = 0
        while (i < dim) {
          val value = valueAt(i)
          dot += value * queryVector(i)
          norm += value * value
          i += 1
        }
        val denom = math.sqrt(norm) * queryNorm
        if (denom == 0.0d) 1.0d else math.min(2.0d, math.max(0.0d, 1.0d - dot / denom))
      case DistanceMetric.DOT_PRODUCT =>
        var dot = 0.0d
        var i = 0
        while (i < dim) {
          dot += valueAt(i) * queryVector(i)
          i += 1
        }
        -dot
    }
  }

  private final class InternalRowMaterializer(
      requestedParquetSchema: MessageType,
      outputSchema: StructType,
      embeddingCol: String,
      distanceOrdinal: Int) extends RecordMaterializer[InternalRow] {

    private val values = new Array[Any](outputSchema.length)
    private var current: InternalRow = _
    private var vectorBytes: Array[Byte] = _

    private val root = new GroupConverter {
      private val converters = requestedParquetSchema.getFields.asScala.map(converterFor).toIndexedSeq

      override def getConverter(fieldIndex: Int): Converter =
        converters(fieldIndex)

      override def start(): Unit = {
        var idx = 0
        while (idx < values.length) {
          values(idx) = null
          idx += 1
        }
        vectorBytes = null
      }

      override def end(): Unit = {
        if (vectorBytes == null) {
          throw new IllegalStateException(s"Vector column '$embeddingCol' was not decoded for positional exact fetch.")
        }
        val scoreStartNs = System.nanoTime()
        values(distanceOrdinal) = scoreVector(vectorBytes)
        scoreMs += elapsedMs(scoreStartNs)
        current = new GenericInternalRow(values.clone())
      }
    }

    override def getCurrentRecord: InternalRow = current

    override def getRootConverter: GroupConverter = root

    private def converterFor(field: org.apache.parquet.schema.Type): PrimitiveConverter = {
      val name = field.getName
      new PrimitiveConverter {
        override def addBinary(value: Binary): Unit = {
          if (name == embeddingCol) {
            vectorBytes = value.getBytes
          } else {
            setOutputValue(name, value.getBytes)
          }
        }

        override def addInt(value: Int): Unit = setOutputValue(name, value)

        override def addLong(value: Long): Unit = setOutputValue(name, value)

        override def addFloat(value: Float): Unit = setOutputValue(name, value)

        override def addDouble(value: Double): Unit = setOutputValue(name, value)

        override def addBoolean(value: Boolean): Unit = setOutputValue(name, value)
      }
    }

    private def setOutputValue(name: String, raw: Any): Unit = {
      if (name != embeddingCol && name != HoodieVectorSearchPlanBuilder.DISTANCE_COL) {
        val ordinal = outputSchema.fieldIndex(name)
        values(ordinal) = outputSchema.fields(ordinal).dataType match {
          case StringType => raw match {
            case bytes: Array[Byte] => UTF8String.fromBytes(bytes)
            case other => UTF8String.fromString(String.valueOf(other))
          }
          case BinaryType => raw.asInstanceOf[Array[Byte]]
          case IntegerType => raw.asInstanceOf[Int]
          case LongType => raw.asInstanceOf[Long]
          case FloatType => raw.asInstanceOf[Float]
          case DoubleType => raw.asInstanceOf[Double]
          case BooleanType => raw.asInstanceOf[Boolean]
          case ByteType => raw.asInstanceOf[Int].toByte
          case ShortType => raw.asInstanceOf[Int].toShort
          case other => throw new UnsupportedOperationException(
            s"Unsupported exact positional fetch output type for field '$name': $other")
        }
      }
    }
  }
}

private[analysis] object PositionalParquetFetcher {
  private val LOG = LoggerFactory.getLogger(getClass)
  private val FOOTER_CACHE = new ConcurrentHashMap[String, ParquetMetadata]()
  private val OFFSET_INDEX_CACHE = new ConcurrentHashMap[String, Option[OffsetIndex]]()
  private val RANGE_MERGE_GAP_BYTES = 512L * 1024L

  final case class FetchResult(rows: Iterator[InternalRow], metrics: FetchMetrics)

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
    val cached = FOOTER_CACHE.get(cacheKey)
    if (cached != null) {
      FooterResult(cached, cacheHit = true)
    } else {
      val metadata = ParquetFileReader.readFooter(conf, path)
      val prior = FOOTER_CACHE.putIfAbsent(cacheKey, metadata)
      FooterResult(if (prior == null) metadata else prior, cacheHit = prior != null)
    }
  }

  private def offsetIndex(
      reader: ParquetFileReader,
      fileCacheKey: String,
      rowGroupOrdinal: Int,
      chunk: ColumnChunkMetaData): OffsetIndex = {
    val key = s"$fileCacheKey#$rowGroupOrdinal#${chunk.getPath.toDotString}"
    OFFSET_INDEX_CACHE.computeIfAbsent(key, _ => Option(reader.readOffsetIndex(chunk))).orNull
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
