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

package org.apache.spark.sql.hudi.blob

import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.io.SeekableDataInputStream
import org.apache.hudi.storage.{HoodieStorage, HoodieStorageUtils, StorageConfiguration, StoragePath}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoders, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, SpecificInternalRow}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

/**
 * Batched byte range reader that optimizes I/O by combining consecutive reads.
 *
 * This reader analyzes sequences of read requests within a partition and merges
 * consecutive or nearby reads into single I/O operations. This significantly reduces
 * the number of seeks and reads when processing sorted data.
 *
 * <h3>Schema Requirement:</h3>
 * The blob column must match the schema defined in {@link org.apache.hudi.common.schema.HoodieSchema.Blob}:
 * <pre>
 * struct {
 *   type: string                   // "inline" or "out_of_line"
 *   bytes: binary (nullable)       // inline data (null for out_of_line)
 *   reference: struct (nullable) { // file reference (null for inline)
 *     external_path: string
 *     offset: long
 *     length: long
 *     managed: boolean
 *   }
 * }
 * </pre>
 *
 * <h3>Key Features:</h3>
 * <ul>
 *   <li>Batches consecutive reads from the same file</li>
 *   <li>Configurable gap threshold for merging nearby reads</li>
 *   <li>Lookahead buffer to identify batch opportunities</li>
 *   <li>Preserves input row order in output</li>
 * </ul>
 *
 * <h3>Usage Example:</h3>
 * {{{
 * import org.apache.hudi.udf.BatchedByteRangeReader
 * import org.apache.spark.sql.functions._
 * // Read a table with a blob column (e.g. image_data)
 * val df = spark.read.format("hudi").load("/my_path").select("image_data", "record_id")
 *
 * // Read with batching (best when data is sorted by external_path, offset)
 * val result = BatchedByteRangeReader.readBatched(df, structColName = "file_info")
 *
 * // Result has: image_data, record_id, data
 * result.show()
 * }}}
 *
 * <h3>Performance Tips:</h3>
 * <ul>
 *   <li>Sort input by (blob.reference.external_path, blob.reference.offset) for maximum batching effectiveness</li>
 *   <li>Increase lookaheadSize for better batch detection (at cost of memory)</li>
 *   <li>Tune maxGapBytes based on your data access patterns</li>
 * </ul>
 *
 * @param storage        HoodieStorage instance for file I/O
 * @param maxGapBytes    Maximum gap between ranges to consider for batching (default: 4KB)
 * @param lookaheadSize  Number of rows to buffer for batch detection (default: 50)
 */
class BatchedByteRangeReader(
    storage: HoodieStorage,
    maxGapBytes: Int = 4096,
    lookaheadSize: Int = 50) {

  private val logger = LoggerFactory.getLogger(classOf[BatchedByteRangeReader])

  /**
   * Process a partition iterator, batching consecutive reads.
   *
   * This method consumes the input iterator and produces an output iterator
   * with each row containing the original data plus a "data" column with the
   * bytes read from the file.
   *
   * @param rows         Iterator of input rows with struct column
   * @param structColIdx Index of the struct column in the row
   * @param outputSchema Schema for output rows
   * @param accessor     Type class for accessing row fields
   * @param builder      Type class for building output rows
   * @tparam R           Row type (Row or InternalRow)
   * @return Iterator of output rows with data column added
   */
  def processPartition[R](
      rows: Iterator[R],
      structColIdx: Int,
      outputSchema: StructType)
      (implicit accessor: RowAccessor[R], builder: RowBuilder[R]): Iterator[R] = {

    // Create buffered iterator for lookahead
    val bufferedRows = rows.buffered

    // Result buffer to maintain order
    val resultIterator = new Iterator[R] {
      private var currentBatch: Iterator[R] = Iterator.empty
      private var rowIndex = 0L

      override def hasNext: Boolean = {
        if (currentBatch.hasNext) {
          true
        } else if (bufferedRows.hasNext) {
          // Process next batch
          currentBatch = processNextBatch()
          currentBatch.hasNext
        } else {
          false
        }
      }

      override def next(): R = {
        if (!hasNext) {
          throw new NoSuchElementException("No more rows")
        }
        currentBatch.next()
      }

      /**
       * Collect and process the next batch of rows.
       */
      private def processNextBatch(): Iterator[R] = {
        // Collect up to lookaheadSize rows with their original indices
        val batch = collectBatch()

        if (batch.isEmpty) {
          Iterator.empty
        } else {

          // Identify and merge consecutive ranges
          val mergedRanges = identifyConsecutiveRanges(batch)

          // Read and split each merged range
          val results = mergedRanges.flatMap { range =>
            readAndSplitRange(range, outputSchema)
          }

          // Sort by original index to preserve input order
          results.sortBy(_.index).map(_.row).iterator
        }
      }

      /**
       * Collect up to lookaheadSize rows from the input iterator.
       */
      private def collectBatch(): Seq[RowInfo[R]] = {
        val batch = ArrayBuffer[RowInfo[R]]()
        var collected = 0

        while (bufferedRows.hasNext && collected < lookaheadSize) {
          val row = bufferedRows.next()
          val blobStruct = accessor.getStruct(row, structColIdx, HoodieSchema.Blob.getBlobFieldCount)

          // Extract blob reference from nested structure
          // structValue has: type (0), data (1), reference (2)
          val referenceStruct = accessor.getStruct(blobStruct, 2, HoodieSchema.Blob.getBlobReferenceFieldCount)  // Get reference struct
          val filePath = accessor.getString(referenceStruct, 0)     // file field
          val offset = accessor.getLong(referenceStruct, 1)         // offset field
          val length = accessor.getLong(referenceStruct, 2).toInt   // length field (Long in schema, cast to Int)

          batch += RowInfo[R](
            originalRow = row,
            filePath = filePath,
            offset = offset,
            length = length,
            index = rowIndex
          )

          rowIndex += 1
          collected += 1
        }

        batch.toSeq
      }
    }

    resultIterator
  }

  /**
   * Identify consecutive ranges that can be batched together.
   *
   * This method groups rows by file path, sorts by offset, and merges
   * ranges that are consecutive or within maxGapBytes of each other.
   *
   * @param rows Sequence of row information
   * @return Sequence of merged ranges
   */
  private def identifyConsecutiveRanges[R](rows: Seq[RowInfo[R]]): Seq[MergedRange[R]] = {
    // Group by file path
    val byFile = rows.groupBy(_.filePath)

    val allRanges = ArrayBuffer[MergedRange[R]]()

    byFile.foreach { case (filePath, fileRows) =>
      // Sort by offset
      val sorted = fileRows.sortBy(_.offset)

      // Merge consecutive ranges
      val merged = mergeRanges(sorted, maxGapBytes)
      allRanges ++= merged
    }

    allRanges.toSeq
  }

  /**
   * Merge consecutive ranges within the gap threshold.
   *
   * @param rows   Sorted rows from the same file
   * @param maxGap Maximum gap to consider for merging
   * @return Sequence of merged ranges
   */
  private def mergeRanges[R](rows: Seq[RowInfo[R]], maxGap: Int): Seq[MergedRange[R]] = {

    val result = ArrayBuffer[MergedRange[R]]()
    var current: MergedRange[R] = null

    rows.foreach { row =>
      if (current == null) {
        // Start first range
        current = MergedRange[R](
          filePath = row.filePath,
          startOffset = row.offset,
          endOffset = row.offset + row.length,
          rows = Seq(row)
        )
      } else {
        val gap = row.offset - current.endOffset

        if (gap >= 0 && gap <= maxGap) {
          // Merge into current range
          current = current.merge(row)
        } else {
          // Save current range and start new one
          result += current
          current = MergedRange[R](
            filePath = row.filePath,
            startOffset = row.offset,
            endOffset = row.offset + row.length,
            rows = Seq(row)
          )
        }
      }
    }

    // Add final range
    if (current != null) {
      result += current
    }

    result.toSeq
  }

  /**
   * Read a merged range and split it back into individual row results.
   *
   * This method performs a single I/O operation to read the entire merged
   * range, then splits the buffer into individual results for each original
   * row.
   *
   * @param range        The merged range to read
   * @param outputSchema Schema for output rows
   * @param builder      Type class for building output rows
   * @tparam R           Row type (Row or InternalRow)
   * @return Sequence of row results with original indices
   */
  private def readAndSplitRange[R](
      range: MergedRange[R],
      outputSchema: StructType)
      (implicit builder: RowBuilder[R]): Seq[RowResult[R]] = {

    var inputStream: SeekableDataInputStream = null
    try {
      // Get or open file handle
      inputStream = storage.openSeekable(new StoragePath(range.filePath), false)

      // Seek to start offset
      inputStream.seek(range.startOffset)

      // Read the entire merged range
      val totalLength = (range.endOffset - range.startOffset).toInt
      val buffer = new Array[Byte](totalLength)
      inputStream.readFully(buffer, 0, totalLength)

      logger.debug(
        s"Read ${totalLength} bytes from ${range.filePath} at offset ${range.startOffset} " +
        s"for ${range.rows.length} rows"
      )

      // Split buffer into individual results
      range.rows.map { rowInfo =>
        val relativeOffset = (rowInfo.offset - range.startOffset).toInt
        val data = buffer.slice(relativeOffset, relativeOffset + rowInfo.length.toInt)

        // Build output row using type class
        val outputRow = builder.buildRow(rowInfo.originalRow, data, outputSchema)

        RowResult[R](
          row = outputRow,
          index = rowInfo.index
        )
      }

    } catch {
      case e: Exception =>
        logger.error(
          s"Failed to read range from ${range.filePath} " +
          s"at offset ${range.startOffset}, length ${range.endOffset - range.startOffset}",
          e
        )
        throw e
    } finally {
      if (inputStream != null) {
        try {
          inputStream.close()
        } catch {
          case e: Exception =>
            logger.warn(s"Error closing input stream for ${range.filePath}", e)
        }
      }
    }
  }
}

/**
 * Type class for accessing row fields.
 * Abstracts over Row and InternalRow API differences.
 *
 * @tparam R Row type (Row or InternalRow)
 */
private[blob] trait RowAccessor[R] {
  def getStruct(row: R, structColIdx: Int, numFields: Int): R
  def getString(struct: R, fieldIdx: Int): String
  def getLong(struct: R, fieldIdx: Int): Long
}

/**
 * Type class for building output rows.
 * Abstracts over Row and InternalRow construction.
 *
 * @tparam R Row type (Row or InternalRow)
 */
private[blob] trait RowBuilder[R] {
  def buildRow(originalRow: R, data: Array[Byte], outputSchema: StructType): R
}

/**
 * Type class instances for Row.
 */
private[blob] object RowAccessor {
  implicit val rowAccessor: RowAccessor[Row] = new RowAccessor[Row] {
    override def getStruct(row: Row, structColIdx: Int, numFields: Int): Row = row.getStruct(structColIdx)
    override def getString(struct: Row, fieldIdx: Int): String = struct.getString(fieldIdx)
    override def getLong(struct: Row, fieldIdx: Int): Long = struct.getLong(fieldIdx)
  }

  implicit val internalRowAccessor: RowAccessor[InternalRow] = new RowAccessor[InternalRow] {
    override def getStruct(row: InternalRow, structColIdx: Int, numFields: Int): InternalRow =
      row.getStruct(structColIdx, numFields)

    override def getString(struct: InternalRow, fieldIdx: Int): String =
      struct.getUTF8String(fieldIdx).toString

    override def getLong(struct: InternalRow, fieldIdx: Int): Long =
      struct.getLong(fieldIdx)
  }
}

/**
 * Type class instances for Row builders.
 */
private[blob] object RowBuilder {
  implicit val rowBuilder: RowBuilder[Row] = new RowBuilder[Row] {
    override def buildRow(originalRow: Row, data: Array[Byte], outputSchema: StructType): Row = {
      val outputValues = new Array[Any](originalRow.length + 1)
      var i = 0
      while (i < originalRow.length) {
        outputValues(i) = originalRow.get(i)
        i += 1
      }
      outputValues(originalRow.length) = data
      new GenericRowWithSchema(outputValues, outputSchema)
    }
  }

  implicit val internalRowBuilder: RowBuilder[InternalRow] = new RowBuilder[InternalRow] {
    override def buildRow(originalRow: InternalRow, data: Array[Byte], outputSchema: StructType): InternalRow = {
      val outputRow = new SpecificInternalRow(outputSchema.fields.map(_.dataType))
      var i = 0
      while (i < originalRow.numFields) {
        if (originalRow.isNullAt(i)) {
          outputRow.setNullAt(i)
        } else {
          val dataType = outputSchema.fields(i).dataType
          // Copy field using generic get/update for compatibility
          outputRow.update(i, originalRow.get(i, dataType))
        }
        i += 1
      }
      // Set the data field (last position)
      outputRow.update(originalRow.numFields, data)
      outputRow
    }
  }
}

/**
 * Information about a single row to be read.
 *
 * @param originalRow Original input row
 * @param filePath    Path to the file
 * @param offset      Byte offset in file
 * @param length      Number of bytes to read
 * @param index       Original position in input (for ordering)
 * @tparam R          Row type (Row or InternalRow)
 */
private case class RowInfo[R](
    originalRow: R,
    filePath: String,
    offset: Long,
    length: Long,
    index: Long)

/**
 * A merged range combining multiple consecutive reads.
 *
 * @param filePath    Path to the file
 * @param startOffset Start byte offset of merged range
 * @param endOffset   End byte offset of merged range (exclusive)
 * @param rows        Individual rows included in this range
 * @tparam R          Row type (Row or InternalRow)
 */
private case class MergedRange[R](
    filePath: String,
    startOffset: Long,
    endOffset: Long,
    rows: Seq[RowInfo[R]]) {

  /**
   * Merge another row into this range.
   *
   * @param row Row to merge
   * @return New merged range including the row
   */
  def merge(row: RowInfo[R]): MergedRange[R] = {
    copy(
      endOffset = math.max(endOffset, row.offset + row.length),
      rows = rows :+ row
    )
  }
}

/**
 * Result row with its original index for ordering.
 *
 * @param row   Output row with data
 * @param index Original position in input
 * @tparam R    Row type (Row or InternalRow)
 */
private case class RowResult[R](
    row: R,
    index: Long)

/**
 * Companion object providing the main API for batched byte range reading.
 */
object BatchedByteRangeReader {

  private val logger = LoggerFactory.getLogger(BatchedByteRangeReader.getClass)

  /** Default maximum gap to consider for batching */
  val DEFAULT_MAX_GAP_BYTES = 4096

  /** Default lookahead buffer size */
  val DEFAULT_LOOKAHEAD_SIZE = 50

  val DATA_COL = "__temp__data"

  /**
   * Read byte ranges from a DataFrame with a Blob column.
   *
   * The struct column must contain the HoodieSchema.Blob structure:
   * - storage_type (String): "out_of_line" or "inline"
   * - bytes (Binary, nullable): inline blob data or null
   * - reference (Struct, nullable): out-of-line reference with external_path, offset, length, and managed fields
   * Returns a DataFrame with all original columns plus a "data" column containing byte arrays.
   *
   * For best performance, sort the input DataFrame by the struct fields (file_path, offset)
   * before calling this method.
   *
   * @param df             Input DataFrame with struct column
   * @param storageConf    Storage configuration for file access
   * @param maxGapBytes    Max gap to consider consecutive (default: 4096)
   * @param lookaheadSize  Rows to buffer for batching (default: 50)
   * @param columnName     Optional column name to resolve. If not provided, searches for column with hudi_blob=true metadata
   * @param keepTempColumn If true, keeps __temp__data column; if false (default), renames it to original column name
   * @return DataFrame with struct column + data column
   * @throws IllegalArgumentException if struct column is missing or has wrong schema
   */
  def readBatched(
      df: Dataset[Row],
      storageConf: StorageConfiguration[_],
      maxGapBytes: Int = DEFAULT_MAX_GAP_BYTES,
      lookaheadSize: Int = DEFAULT_LOOKAHEAD_SIZE,
      columnName: Option[String] = None,
      keepTempColumn: Boolean = false): Dataset[Row] = {

    require(maxGapBytes >= 0, "maxGapBytes must be non-negative")
    require(lookaheadSize > 0, "lookaheadSize must be positive")

    val spark = df.sparkSession

    // Get struct column index - use provided column name or fallback to metadata search
    val (structColIdx, structColName) = columnName match {
      case Some(name) =>
        // Use provided column name directly
        val idx = df.schema.fieldIndex(name)
        (idx, name)
      case None =>
        // Fallback to metadata-based inference
        getBlobColumn(df.schema)
    }

    // Create output schema (input + data column)
    val outputSchema = df.schema.add(StructField(DATA_COL, BinaryType, nullable = false))

    // Broadcast storage configuration
    val broadcastConf = spark.sparkContext.broadcast(storageConf)

    // Apply mapPartitions
    val result = df.mapPartitions { partition =>
      try {
        // Create storage and reader for this partition
        val storage = HoodieStorageUtils.getStorage(broadcastConf.value)
        val reader = new BatchedByteRangeReader(storage, maxGapBytes, lookaheadSize)

        // Import implicit instances for Row
        import RowAccessor.rowAccessor
        import RowBuilder.rowBuilder

        // Process partition
        reader.processPartition[Row](partition, structColIdx, outputSchema)

      } catch {
        case e: Exception =>
          logger.error("Error processing partition", e)
          throw e
      }
    } (Encoders.row(outputSchema))

    if (keepTempColumn) {
      // Keep both columns for ReadBlobRule
      result
    } else {
      // Backwards compatible behavior: rename __temp__data to original column name
      result.drop(structColName).withColumnRenamed(DATA_COL, structColName)
    }
  }

  /**
   * Process RDD[InternalRow] directly without DataFrame conversion.
   *
   * This method provides optimized processing for physical plan execution,
   * avoiding the overhead of RDD → DataFrame → RDD conversions.
   *
   * @param rdd           Input RDD of InternalRows
   * @param schema        Schema of the input RDD
   * @param storageConf   Storage configuration for file access
   * @param maxGapBytes   Max gap to consider consecutive (default: 4096)
   * @param lookaheadSize Rows to buffer for batching (default: 50)
   * @param columnName    Optional column name to resolve. If not provided, searches for column with hudi_blob=true metadata
   * @return RDD with struct column + data column
   * @throws IllegalArgumentException if struct column is missing or has wrong schema
   */
  def processRDD(
      rdd: RDD[InternalRow],
      schema: StructType,
      storageConf: StorageConfiguration[_],
      maxGapBytes: Int = DEFAULT_MAX_GAP_BYTES,
      lookaheadSize: Int = DEFAULT_LOOKAHEAD_SIZE,
      columnName: String): RDD[InternalRow] = {

    require(maxGapBytes >= 0, "maxGapBytes must be non-negative")
    require(lookaheadSize > 0, "lookaheadSize must be positive")

    // Get struct column index
    val structColIdx = schema.fieldIndex(columnName)

    // Create output schema (input + __temp__data column)
    val outputSchema = schema.add(StructField(DATA_COL, BinaryType, nullable = false))

    // Broadcast configuration
    val broadcastConf = rdd.sparkContext.broadcast(storageConf)

    // Process partitions using InternalRow type classes
    rdd.mapPartitions { partition =>
      val storage = HoodieStorageUtils.getStorage(broadcastConf.value)
      val reader = new BatchedByteRangeReader(storage, maxGapBytes, lookaheadSize)

      // Import implicit instances for InternalRow
      import RowAccessor.internalRowAccessor
      import RowBuilder.internalRowBuilder

      reader.processPartition[InternalRow](partition, structColIdx, outputSchema)
    }
  }

  /**
   * Find the blob column by searching for metadata hudi_blob=true.
   *
   * This is a fallback method used when no explicit column name is provided.
   * Searches through all fields in the schema to find the first field
   * that has metadata with key "hudi_blob" set to boolean true.
   *
   * @param structType The schema to search
   * @return The index of the blob column and the name of the column
   * @throws IllegalArgumentException if no blob column is found
   */
  private def getBlobColumn(structType: StructType): (Int, String) = {
    // Find field with metadata key hudi_blob=true
    val blobFieldIndex = structType.fields.zipWithIndex.find { case (field, _) =>
      field.metadata.contains("hudi_blob") && field.metadata.getBoolean("hudi_blob")
    }.map(fieldAndIndex => (fieldAndIndex._2, fieldAndIndex._1.name))

    blobFieldIndex.getOrElse {
      throw new IllegalArgumentException(
        s"No blob column found with metadata hudi_blob=true. " +
        s"Available columns: ${structType.fieldNames.mkString(", ")}"
      )
    }
  }
}
