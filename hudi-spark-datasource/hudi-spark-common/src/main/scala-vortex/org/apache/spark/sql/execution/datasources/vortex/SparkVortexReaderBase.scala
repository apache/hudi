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

package org.apache.spark.sql.execution.datasources.vortex

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.schema.internal.InternalSchema
import org.apache.hudi.common.util
import org.apache.hudi.common.util.collection.{ClosableIterator, Pair}
import org.apache.hudi.io.memory.HoodieArrowAllocator
import org.apache.hudi.io.storage.{VortexArrowColumnVector, VortexArrowUtils, VortexBatchIterator, VortexRecordIterator, VortexScans}
import org.apache.hudi.io.vortex.{VortexArrowProperties, VortexInputFile}
import org.apache.hudi.storage.StorageConfiguration

import dev.vortex.api.ScanOptions
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.RowToColumnConverter
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader, SparkSchemaTransformUtils}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import java.io.IOException

import scala.collection.JavaConverters._

/**
 * Reader for Vortex files in the Spark datasource query path. Opens the file through
 * [[VortexInputFile]] and converts Arrow batches to Spark rows through [[VortexRecordIterator]].
 *
 * The scan reads only the requested columns. It evaluates the filters that Vortex supports, and
 * it reads only the rows of its split when Spark splits the file. The Hudi row index column gets
 * the position of each row in the file.
 *
 * When `returningBatch` is true, the reader returns [[ColumnarBatch]]es. A batch is a view of the
 * Arrow data, without a copy, if the types of the file columns are the requested types. Otherwise
 * the reader converts the rows to batches.
 *
 * When the scan is vectorized but Spark wants rows, for example for the base files of a merge on
 * read table, the reader returns the rows of the batch views, as the Parquet reader does. Spark
 * copies each row once, before the next batch replaces the data.
 *
 * @param enableVectorizedReader whether the scan can read vectorized
 * @param returningBatch         whether the reader returns batches instead of rows
 */
class SparkVortexReaderBase(enableVectorizedReader: Boolean, returningBatch: Boolean)
  extends SparkColumnarFileReader {

  // Set the Arrow properties before the reader uses Arrow buffers and vectors.
  VortexArrowProperties.ensureConfigured()

  def this(enableVectorizedReader: Boolean) = this(enableVectorizedReader, false)

  /**
   * Read a Vortex file with schema projection and partition column support.
   *
   * @param file              Vortex file to read
   * @param requiredSchema    desired output schema of the data (columns to read)
   * @param partitionSchema   schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt option of internal schema for schema.on.read (not currently used for Vortex)
   * @param filters           filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param storageConf       the hadoop conf
   * @param tableSchemaOpt    option of table schema for timestamp precision conversion fix.
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[org.apache.spark.sql.vectorized.ColumnarBatch]]
   */
  override def read(file: PartitionedFile,
                    requiredSchema: StructType,
                    partitionSchema: StructType,
                    internalSchemaOpt: util.Option[InternalSchema],
                    filters: scala.Seq[Filter],
                    storageConf: StorageConfiguration[Configuration],
                    tableSchemaOpt: util.Option[MessageType] = util.Option.empty()): Iterator[InternalRow] = {

    val path = sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file).toString

    if (requiredSchema.isEmpty && partitionSchema.isEmpty) {
      // No columns requested - return empty iterator
      Iterator.empty
    } else {

      val dataAllocatorSize = storageConf.unwrap().getLong(
        HoodieStorageConfig.VORTEX_READ_ALLOCATOR_SIZE_BYTES.key(),
        HoodieStorageConfig.VORTEX_READ_ALLOCATOR_SIZE_BYTES.defaultValue().toLong)
      val allocator = HoodieArrowAllocator.newChildAllocator(
        getClass.getSimpleName + "-data-" + path, dataAllocatorSize)

      var vortexIterator: ClosableIterator[UnsafeRow] = null
      var arrowBatches: Iterator[InternalRow] = null
      var vortexFile: VortexInputFile = null
      var released = false

      // Closes the file and the allocator when no iterator owns them.
      def release(): Unit = {
        if (!released) {
          released = true
          try {
            if (vortexFile != null) {
              vortexFile.close()
            }
          } finally {
            allocator.close()
          }
        }
      }

      try {
        // Spark gives the file size of each split. Hudi does not always give it, and then 0 means
        // that the size is not known.
        val knownLength = if (file.fileSize > 0) file.fileSize else -1L
        vortexFile = VortexInputFile.open(storageConf.unwrap(), path, knownLength)

        // Convert the Vortex file schema to a Spark schema, then reconcile it against the requested
        // schema for schema evolution (implicit type changes, columns not present in the file).
        val fileSchema = VortexArrowUtils.toStructType(vortexFile.arrowSchema(allocator))
        val (implicitTypeChangeInfo, sparkRequestSchema) =
          SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(fileSchema, requiredSchema)

        // Only read columns that exist in the file, and the row index column. Missing columns are
        // null-padded below.
        val fileColumns = SparkSchemaTransformUtils.filterSchemaByFileSchema(sparkRequestSchema, fileSchema)
        val rowIndexFields = requiredSchema.fields.filter(f => VortexScans.isRowIndexColumn(f.name))
        val requestSchema = StructType(fileColumns.fields ++ rowIndexFields)

        val rowRange = splitRowRange(file, vortexFile)
        val rowsInRange = rowRange.map(range => range(1) - range(0))

        val baseIter: Iterator[InternalRow] = if (rowsInRange.contains(0L)) {
          // The split has no rows. A scan of the row range 0 to 0 reads the whole file.
          release()
          Iterator.empty
        } else if (requestSchema.isEmpty) {
          // No data columns to read from the file (only partition columns requested, or all
          // requested columns are new via schema evolution). Emit one empty row per record so the
          // null-padding and partition projections can fill in the values.
          val numRows = rowsInRange.getOrElse(vortexFile.exactRowCount().orElse(0L))
          release()
          if (returningBatch) {
            // Constant batches do not need a projection or a conversion for each row.
            arrowBatches = VortexBatchIterator.constantBatches(
              requiredSchema, partitionSchema, file.partitionValues, numRows).asScala.asInstanceOf[Iterator[InternalRow]]
          }
          val emptyRow = InternalRow.empty
          // Emit numRows empty rows without narrowing the long count to int (a >2^31-row file
          // would otherwise overflow and silently yield zero rows).
          new Iterator[InternalRow] {
            private var remaining = numRows
            override def hasNext: Boolean = remaining > 0
            override def next(): InternalRow = {
              remaining -= 1
              emptyRow
            }
          }
        } else {
          val options = ScanOptions.builder().projection(VortexScans.projection(requestSchema, fileSchema))
          rowRange.foreach(range => options.rowRangeBegin(range(0)).rowRangeEnd(range(1)))

          // Schema on read can rename columns, thus the file names are not the query names.
          if (!internalSchemaOpt.isPresent) {
            VortexScans.filter(filters.asJava, requestSchema, fileSchema).ifPresent(f => options.filter(f))
          }

          val scan = vortexFile.scan(options.build())
          if (readsArrowBatches(requestSchema, requiredSchema, implicitTypeChangeInfo)) {
            // The batches are views of the Arrow data. The batches give null for the requested
            // columns that are not in the file, and the partition values of the file.
            val batches = new VortexBatchIterator(
              allocator, vortexFile, scan, requiredSchema, partitionSchema, file.partitionValues)
            released = true
            Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => batches.close()))

            arrowBatches = if (returningBatch) {
              batches.asScala.asInstanceOf[Iterator[InternalRow]]
            } else {
              batches.asScala.flatMap(_.rowIterator().asScala)
            }
            Iterator.empty
          } else {
            vortexIterator = new VortexRecordIterator(allocator, vortexFile, scan, requestSchema)
            released = true

            // Register cleanup listener
            Option(TaskContext.get()).foreach { ctx =>
              ctx.addTaskCompletionListener[Unit](_ => vortexIterator.close())
            }

            vortexIterator.asScala
          }
        }

        if (arrowBatches != null) {
          arrowBatches
        } else {
          toOutputRows(baseIter, requestSchema, requiredSchema, partitionSchema, implicitTypeChangeInfo,
            file.partitionValues)
        }

      } catch {
        case e: Exception =>
          if (vortexIterator != null) {
            vortexIterator.close() // Closes the iterator, which owns the file and allocator lifecycle
          } else {
            try {
              release()
            } catch {
              case closeFailure: Exception => e.addSuppressed(closeFailure)
            }
          }
          throw new IOException(s"Failed to read Vortex file: $path", e)
      }
    }
  }

  /**
   * Pads the missing columns with null, casts the columns with changed types, and appends the
   * partition values. When the reader returns batches, it converts the rows to batches.
   */
  private def toOutputRows(baseIter: Iterator[InternalRow],
                           requestSchema: StructType,
                           requiredSchema: StructType,
                           partitionSchema: StructType,
                           implicitTypeChangeInfo: java.util.Map[Integer, Pair[DataType, DataType]],
                           partitionValues: InternalRow): Iterator[InternalRow] = {
    // Create the following projections for schema evolution:
    // 1. Padding projection: add NULL for missing columns
    // 2. Casting projection: handle implicit type conversions
    val schemaUtils = sparkAdapter.getSchemaUtils
    val paddingProj = SparkSchemaTransformUtils.generateNullPaddingProjection(requestSchema, requiredSchema)
    val castProj = SparkSchemaTransformUtils.generateUnsafeProjection(
      schemaUtils.toAttributes(requiredSchema),
      Some(SQLConf.get.sessionLocalTimeZone),
      implicitTypeChangeInfo,
      requiredSchema,
      new StructType(),
      schemaUtils
    )

    // Unify projections by applying padding and then casting for each row
    val projection: UnsafeProjection = new UnsafeProjection {
      def apply(row: InternalRow): UnsafeRow =
        castProj(paddingProj(row))
    }
    val projectedIter = baseIter.map(projection.apply)

    // Handle partition columns
    val rows = if (partitionSchema.length == 0) {
      // No partition columns - return rows directly
      projectedIter
    } else {
      // Create UnsafeProjection to convert JoinedRow to UnsafeRow
      val fullSchema = (requiredSchema.fields ++ partitionSchema.fields).map(f =>
        AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

      // Append partition values to each row using JoinedRow, then convert to UnsafeRow
      val joinedRow = new JoinedRow()
      projectedIter.map(row => unsafeProjection(joinedRow(row, partitionValues)))
    }

    if (returningBatch) {
      toBatches(rows, StructType(requiredSchema.fields ++ partitionSchema.fields))
    } else {
      rows
    }
  }

  /**
   * Returns true when the reader can read the scan as batch views: the scan is vectorized, the
   * column views can give each file column as the requested type (for example, a smallint file
   * column as an int), and each scanned column has the name of a requested column.
   */
  private def readsArrowBatches(requestSchema: StructType,
                                requiredSchema: StructType,
                                implicitTypeChangeInfo: java.util.Map[Integer, Pair[DataType, DataType]]): Boolean = {
    val requiredNames = requiredSchema.fieldNames.toSet
    // Each type change maps the requested type to the type of the file column.
    val viewsServeTypeChanges = implicitTypeChangeInfo.values().asScala.forall(change =>
      VortexArrowColumnVector.canConformTo(change.getRight, change.getLeft))
    (returningBatch || enableVectorizedReader) && viewsServeTypeChanges &&
      requestSchema.fieldNames.forall(requiredNames.contains)
  }

  /**
   * Converts rows to batches of at most [[SparkVortexReaderBase.ConvertedBatchSize]] rows. The
   * batches use the same column vectors again, thus a batch is valid only until the next batch.
   */
  private def toBatches(rows: Iterator[InternalRow], schema: StructType): Iterator[InternalRow] = {
    val vectors: Array[WritableColumnVector] =
      OnHeapColumnVector.allocateColumns(SparkVortexReaderBase.ConvertedBatchSize, schema).map(v => v: WritableColumnVector)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => vectors.foreach(_.close())))
    val converter = new RowToColumnConverter(schema)

    new Iterator[ColumnarBatch] {
      private val batch = new ColumnarBatch(vectors.map(v => v: ColumnVector))

      override def hasNext: Boolean = rows.hasNext

      override def next(): ColumnarBatch = {
        vectors.foreach(_.reset())
        var numRows = 0
        while (numRows < SparkVortexReaderBase.ConvertedBatchSize && rows.hasNext) {
          converter.convert(rows.next(), vectors)
          numRows += 1
        }
        batch.setNumRows(numRows)
        batch
      }
    }.asInstanceOf[Iterator[InternalRow]]
  }

  /**
   * Returns the rows of the split as `[begin, end)`, or None when the split reads the whole file.
   *
   * Without an exact row count, splits cannot agree on their boundaries. Then the split that
   * starts the file reads all rows, and the other splits read none.
   */
  private def splitRowRange(file: PartitionedFile, vortexFile: VortexInputFile): Option[Array[Long]] = {
    val fileSize = vortexFile.length()
    if (VortexScans.isWholeFile(file.start, file.length, fileSize)) {
      None
    } else {
      val rowCount = vortexFile.exactRowCount()
      if (rowCount.isPresent) {
        Some(VortexScans.splitRowRange(file.start, file.length, fileSize, rowCount.getAsLong))
      } else if (file.start == 0) {
        None
      } else {
        Some(Array(0L, 0L))
      }
    }
  }
}

object SparkVortexReaderBase {

  /**
   * Number of rows in a batch that the reader makes from rows.
   */
  private val ConvertedBatchSize = 4096
}
