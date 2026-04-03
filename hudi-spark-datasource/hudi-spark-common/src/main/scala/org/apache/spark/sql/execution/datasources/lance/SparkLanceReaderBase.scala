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

package org.apache.spark.sql.execution.datasources.lance

import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.common.util
import org.apache.hudi.common.util.collection.{ClosableIterator, Pair => HoodiePair}
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.io.memory.HoodieArrowAllocator
import org.apache.hudi.io.storage.{BlobDescriptorTransform, HoodieSparkLanceReader, LanceBatchIterator, LanceRecordIterator, VectorConversionUtils}
import org.apache.hudi.storage.StorageConfiguration

import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader, SparkSchemaTransformUtils}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.LanceArrowUtils
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector, LanceArrowColumnVector}
import org.lance.file.{BlobReadMode, FileReadOptions, LanceFileReader}

import java.io.{Closeable, IOException}

import scala.collection.JavaConverters._

/**
 * Reader for Lance files in Spark datasource.
 * Supports both row-based and columnar batch reading modes.
 *
 * @param enableVectorizedReader when true, returns ColumnarBatch for vectorized processing;
 *                               when false, returns InternalRow one by one
 */
class SparkLanceReaderBase(enableVectorizedReader: Boolean) extends SparkColumnarFileReader {

  // Batch size for reading Lance files (number of rows per batch)
  private val DEFAULT_BATCH_SIZE = 512

  /**
   * Read a Lance file with schema projection and partition column support.
   *
   * @param file              Lance file to read
   * @param requiredSchema    desired output schema of the data (columns to read)
   * @param partitionSchema   schema of the partition columns. Partition values will be appended to the end of every row
   * @param internalSchemaOpt option of internal schema for schema.on.read (not currently used for Lance)
   * @param filters           filters for data skipping. Not guaranteed to be used; the spark plan will also apply the filters.
   * @param storageConf       the hadoop conf
   * @return iterator of rows read from the file output type says [[InternalRow]] but could be [[ColumnarBatch]]
   */
  override def read(file: PartitionedFile,
                    requiredSchema: StructType,
                    partitionSchema: StructType,
                    internalSchemaOpt: util.Option[InternalSchema],
                    filters: scala.Seq[Filter],
                    storageConf: StorageConfiguration[Configuration],
                    tableSchemaOpt: util.Option[MessageType] = util.Option.empty()): Iterator[InternalRow] = {

    val filePath = file.filePath.toString

    if (requiredSchema.isEmpty && partitionSchema.isEmpty) {
      // No columns requested - return empty iterator
      Iterator.empty
    } else {
      // Track iterator for cleanup. Typed as ClosableIterator so we can swap in the
      // DESCRIPTOR-mode iterator when the user opts into that blob read mode.
      var lanceIterator: ClosableIterator[UnsafeRow] = null

      // Create child allocator for reading
      val dataAllocatorSize = storageConf.unwrap().getLong(
        HoodieStorageConfig.LANCE_READ_ALLOCATOR_SIZE_BYTES.key(),
        HoodieStorageConfig.LANCE_READ_ALLOCATOR_SIZE_BYTES.defaultValue().toLong)
      val allocator = HoodieArrowAllocator.newChildAllocator(
        getClass.getSimpleName + "-data-" + filePath, dataAllocatorSize)

      try {
        // Open Lance file reader
        val lanceReader = LanceFileReader.open(filePath, allocator)

        // Get schema from Lance file. lance-spark strips Hudi's VECTOR descriptor during
        // Arrow→Spark conversion but keeps the fixed-size-list dimension on the Spark
        // field metadata; rebuild the descriptor from that, using requiredSchema
        // as the source of truth for which columns are Hudi VECTORs — so non-Hudi fixed-size-lists aren't mis-tagged.
        val arrowSchema = lanceReader.schema()
        val vectorColumnNames: java.util.Set[String] = VectorConversionUtils
          .detectVectorColumnsFromMetadata(requiredSchema)
          .keySet()
          .asScala
          .map(i => requiredSchema.fields(i).name)
          .toSet
          .asJava
        val fileSchema = VectorConversionUtils.restoreVectorMetadata(
          LanceArrowUtils.fromArrowSchema(arrowSchema), vectorColumnNames)

        // Build type change info for schema evolution
        val (implicitTypeChangeInfo, sparkRequestSchema) =
          SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(fileSchema, requiredSchema)

        // Filter schema to only fields that exist in file (Lance can only read columns present in file).
        val requestSchema =
          SparkSchemaTransformUtils.filterSchemaByFileSchema(sparkRequestSchema, fileSchema)

        // Lance returns null BLOB sub-structs as non-null parents with null children; widen
        // nullability inside BLOB subtrees so the codegen projection doesn't NPE on them.
        val iteratorSchema = widenBlobSubtreeNullability(requestSchema)

        val columnNames = if (iteratorSchema.nonEmpty) {
          iteratorSchema.fieldNames.toList.asJava
        } else {
          // If only partition columns requested, read minimal data
          null
        }

        // Honor `hoodie.read.blob.inline.mode`. CONTENT (default) materializes INLINE bytes in
        // the `data` column; DESCRIPTOR surfaces per-row {position, size} which the descriptor
        // iterator rewrites into Hudi OUT_OF_LINE references. Non-blob Lance columns ignore
        // the option regardless.
        val blobMode = resolveBlobReadMode(storageConf)
        val readOpts = FileReadOptions.builder().blobReadMode(blobMode).build()
        val arrowReader = lanceReader.readAll(columnNames, null, DEFAULT_BATCH_SIZE, readOpts)

        // Decide between batch mode and row mode.
        // Fall back to row mode if type casting is needed (batch-level type casting deferred to follow-up).
        val hasTypeChanges = !implicitTypeChangeInfo.isEmpty
        if (enableVectorizedReader && !hasTypeChanges) {
          readBatch(file, allocator, lanceReader, arrowReader, filePath,
            requestSchema, requiredSchema, partitionSchema)
        } else {
          // Compose the DESCRIPTOR-aware blob transform only when the user opted into that mode
          // AND the request actually has BLOB columns (otherwise the rewrite has nothing to do).
          val blobFieldNames: java.util.Set[String] =
            iteratorSchema.fields.collect { case f if isBlobField(f) => f.name }.toSet.asJava
          val blobTransform = if (blobMode == BlobReadMode.DESCRIPTOR && !blobFieldNames.isEmpty) {
            new BlobDescriptorTransform(blobFieldNames, filePath)
          } else {
            null
          }
          val recordIterator = new LanceRecordIterator(
            allocator, lanceReader, arrowReader, iteratorSchema, filePath, blobTransform)
          lanceIterator = recordIterator

          // Register cleanup listener
          Option(TaskContext.get()).foreach { ctx =>
            ctx.addTaskCompletionListener[Unit](_ => recordIterator.close())
          }

          readRows(file, recordIterator, iteratorSchema, requiredSchema, partitionSchema, implicitTypeChangeInfo)
        }

      } catch {
        case e: Exception =>
          if (lanceIterator != null) {
            lanceIterator.close()  // Close iterator which handles lifecycle for all objects
          } else {
            allocator.close()      // Close allocator directly
          }
          throw new IOException(s"Failed to read Lance file: $filePath", e)
      }
    }
  }

  /**
   * Resolve the Lance blob read mode from {@code hoodie.read.blob.inline.mode}. Unknown values
   * fail fast so typos don't silently fall back to the default.
   */
  private def resolveBlobReadMode(storageConf: StorageConfiguration[Configuration]): BlobReadMode = {
    val configured = storageConf.unwrap()
      .get(HoodieReaderConfig.BLOB_INLINE_READ_MODE.key(),
        HoodieReaderConfig.BLOB_INLINE_READ_MODE.defaultValue())
    configured.toUpperCase match {
      case HoodieReaderConfig.BLOB_INLINE_READ_MODE_CONTENT => BlobReadMode.CONTENT
      case HoodieReaderConfig.BLOB_INLINE_READ_MODE_DESCRIPTOR => BlobReadMode.DESCRIPTOR
      case other =>
        throw new IllegalArgumentException(
          s"Unsupported value '$other' for ${HoodieReaderConfig.BLOB_INLINE_READ_MODE.key()}; " +
            s"expected one of [${HoodieReaderConfig.BLOB_INLINE_READ_MODE_CONTENT}, " +
            s"${HoodieReaderConfig.BLOB_INLINE_READ_MODE_DESCRIPTOR}]")
    }
  }

  /**
   * Widens nullability to true only within BLOB subtrees: the BLOB field itself and all of its
   * descendants. Lance can materialize a BLOB's nested struct (e.g. `reference` for an INLINE
   * row) as non-null with all-null leaves, which the downstream codegen projection would NPE
   * on if the Hudi schema declares those leaves non-nullable. Non-blob fields keep their
   * original nullability so their contracts aren't silently loosened.
   */
  private def widenBlobSubtreeNullability(schema: StructType): StructType = {
    StructType(schema.fields.map { f =>
      if (isBlobField(f)) {
        f.copy(nullable = true, dataType = forceTypeNullable(f.dataType))
      } else {
        f
      }
    })
  }

  private def isBlobField(field: StructField): Boolean = {
    val md = field.metadata
    md != null &&
      md.contains(HoodieSchema.TYPE_METADATA_FIELD) &&
      HoodieSchema.parseTypeDescriptor(md.getString(HoodieSchema.TYPE_METADATA_FIELD))
        .getType == HoodieSchemaType.BLOB
  }

  private def forceFieldNullable(field: StructField): StructField =
    field.copy(nullable = true, dataType = forceTypeNullable(field.dataType))

  private def forceTypeNullable(dt: DataType): DataType = dt match {
    case s: StructType => StructType(s.fields.map(forceFieldNullable))
    case a: ArrayType => a.copy(elementType = forceTypeNullable(a.elementType), containsNull = true)
    case m: MapType => m.copy(
      keyType = forceTypeNullable(m.keyType),
      valueType = forceTypeNullable(m.valueType),
      valueContainsNull = true)
    case other => other
  }

  /**
   * Columnar batch reading path. Returns Iterator[ColumnarBatch] type-erased as Iterator[InternalRow].
   * Used when enableVectorizedReader=true and no type casting is needed.
   */
  private def readBatch(file: PartitionedFile,
                        allocator: org.apache.arrow.memory.BufferAllocator,
                        lanceReader: LanceFileReader,
                        arrowReader: ArrowReader,
                        filePath: String,
                        requestSchema: StructType,
                        requiredSchema: StructType,
                        partitionSchema: StructType): Iterator[InternalRow] = {

    val batchIterator = new LanceBatchIterator(allocator, lanceReader, arrowReader, filePath)

    // Build column mapping: for each column in requiredSchema, find its index in requestSchema (file columns)
    // Returns -1 if the column is missing from the file (schema evolution: column addition)
    val columnMapping: Array[Int] = requiredSchema.fields.map { field =>
      requestSchema.fieldNames.indexOf(field.name)
    }

    // Create Arrow-backed null vectors for columns missing from the file.
    // Uses LanceArrowColumnVector so that Spark's vectorTypes() contract is satisfied
    // (FileSourceScanExec expects all data columns to be LanceArrowColumnVector).
    val nullAllocator = if (columnMapping.contains(-1)) {
      HoodieArrowAllocator.newChildAllocator(
        getClass.getSimpleName + "-null-" + filePath, HoodieSparkLanceReader.LANCE_DATA_ALLOCATOR_SIZE)
    } else null

    val nullColumnVectors: Array[(Int, LanceArrowColumnVector, org.apache.arrow.vector.FieldVector)] =
      if (nullAllocator != null) {
        columnMapping.zipWithIndex.filter(_._1 < 0).map { case (_, idx) =>
          val field = LanceArrowUtils.toArrowField(
            requiredSchema(idx).name, requiredSchema(idx).dataType, requiredSchema(idx).nullable, "UTC")
          val arrowVector = field.createVector(nullAllocator)
          arrowVector.allocateNew()
          arrowVector.setValueCount(DEFAULT_BATCH_SIZE)
          (idx, new LanceArrowColumnVector(arrowVector), arrowVector)
        }
      } else {
        Array.empty
      }

    // Pre-create partition column vectors (reused across batches, reset per batch)
    val hasPartitionColumns = partitionSchema.length > 0
    val partitionVectors: Array[WritableColumnVector] = if (hasPartitionColumns) {
      partitionSchema.fields.map(f => new OnHeapColumnVector(DEFAULT_BATCH_SIZE, f.dataType))
    } else {
      Array.empty
    }

    // Populate partition vectors with constant values
    var lastPopulatedNumRows = DEFAULT_BATCH_SIZE
    if (hasPartitionColumns) {
      populatePartitionVectors(partitionVectors, partitionSchema, file.partitionValues, DEFAULT_BATCH_SIZE)
    }

    val totalColumns = requiredSchema.length + partitionSchema.length

    // Map each source batch to a batch with the correct column layout.
    val mappedIterator = new Iterator[ColumnarBatch] with Closeable {
      override def hasNext: Boolean = batchIterator.hasNext()

      override def next(): ColumnarBatch = {
        val sourceBatch = batchIterator.next()
        val numRows = sourceBatch.numRows()

        val vectors = new Array[ColumnVector](totalColumns)

        // Data columns: reorder from source batch or substitute null Arrow vector
        var i = 0
        while (i < requiredSchema.length) {
          if (columnMapping(i) >= 0) {
            vectors(i) = sourceBatch.column(columnMapping(i))
          } else {
            // Find the pre-created null vector for this index
            val entry = nullColumnVectors.find(_._1 == i).get
            // Adjust valueCount if batch size differs from allocated size
            if (numRows != entry._3.getValueCount) {
              entry._3.setValueCount(numRows)
            }
            vectors(i) = entry._2
          }
          i += 1
        }

        // Partition columns: constant vectors
        if (hasPartitionColumns) {
          if (numRows != lastPopulatedNumRows) {
            populatePartitionVectors(partitionVectors, partitionSchema, file.partitionValues, numRows)
            lastPopulatedNumRows = numRows
          }
          var j = 0
          while (j < partitionSchema.length) {
            vectors(requiredSchema.length + j) = partitionVectors(j)
            j += 1
          }
        }

        val result = new ColumnarBatch(vectors)
        result.setNumRows(numRows)
        result
      }

      override def close(): Unit = {
        // Close null Arrow vectors and their allocator before batchIterator (which closes the data allocator)
        nullColumnVectors.foreach { case (_, columnVector, _) =>
          columnVector.close()
        }
        if (nullAllocator != null) nullAllocator.close()
        batchIterator.close()
        partitionVectors.foreach(_.close())
      }
    }

    // Register cleanup listener
    Option(TaskContext.get()).foreach { ctx =>
      ctx.addTaskCompletionListener[Unit](_ => mappedIterator.close())
    }

    mappedIterator.asInstanceOf[Iterator[InternalRow]]
  }

  /**
   * Row-based reading path. Consumes the pre-built [[LanceRecordIterator]] (already wired up
   * with blob transform if applicable) and applies schema-evolution projections + partition handling.
   */
  private def readRows(file: PartitionedFile,
                       recordIterator: LanceRecordIterator,
                       iteratorSchema: StructType,
                       requiredSchema: StructType,
                       partitionSchema: StructType,
                       implicitTypeChangeInfo: java.util.Map[Integer, HoodiePair[DataType, DataType]]): Iterator[InternalRow] = {

    val baseIter: Iterator[InternalRow] = recordIterator.asScala

    // Create the following projections for schema evolution:
    // 1. Padding projection: add NULL for missing columns
    // 2. Casting projection: handle type conversions
    val schemaUtils = sparkAdapter.getSchemaUtils
    val paddingProj = SparkSchemaTransformUtils.generateNullPaddingProjection(iteratorSchema, requiredSchema)
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
    if (partitionSchema.length == 0) {
      // No partition columns - return rows directly
      projectedIter
    } else {
      // Create UnsafeProjection to convert JoinedRow to UnsafeRow
      val fullSchema = (requiredSchema.fields ++ partitionSchema.fields).map(f =>
        AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      val unsafeProjection = GenerateUnsafeProjection.generate(fullSchema, fullSchema)

      // Append partition values to each row using JoinedRow, then convert to UnsafeRow
      val joinedRow = new JoinedRow()
      projectedIter.map(row => unsafeProjection(joinedRow(row, file.partitionValues)))
    }
  }

  /**
   * Populate writable column vectors with constant partition values.
   * Each vector is filled with the same value for all rows.
   */
  private def populatePartitionVectors(vectors: Array[WritableColumnVector],
                                       partitionSchema: StructType,
                                       partitionValues: InternalRow,
                                       numRows: Int): Unit = {
    var i = 0
    while (i < partitionSchema.length) {
      val vector = vectors(i)
      vector.reset()
      if (partitionValues.isNullAt(i)) {
        vector.putNulls(0, numRows)
      } else {
        partitionSchema(i).dataType match {
          case BooleanType =>
            val v = partitionValues.getBoolean(i)
            var j = 0
            while (j < numRows) { vector.putBoolean(j, v); j += 1 }
          case ByteType =>
            val v = partitionValues.getByte(i)
            var j = 0
            while (j < numRows) { vector.putByte(j, v); j += 1 }
          case ShortType =>
            val v = partitionValues.getShort(i)
            var j = 0
            while (j < numRows) { vector.putShort(j, v); j += 1 }
          case IntegerType | DateType =>
            val v = partitionValues.getInt(i)
            vector.putInts(0, numRows, v)
          case LongType | TimestampType =>
            val v = partitionValues.getLong(i)
            vector.putLongs(0, numRows, v)
          case FloatType =>
            val v = partitionValues.getFloat(i)
            var j = 0
            while (j < numRows) { vector.putFloat(j, v); j += 1 }
          case DoubleType =>
            val v = partitionValues.getDouble(i)
            var j = 0
            while (j < numRows) { vector.putDouble(j, v); j += 1 }
          case StringType =>
            val v = partitionValues.getUTF8String(i)
            val bytes = v.getBytes
            var j = 0
            while (j < numRows) { vector.putByteArray(j, bytes); j += 1 }
          case d: DecimalType =>
            val v = partitionValues.getDecimal(i, d.precision, d.scale)
            if (d.precision <= Decimal.MAX_INT_DIGITS) {
              val unscaled = v.toUnscaledLong.toInt
              var j = 0
              while (j < numRows) { vector.putInt(j, unscaled); j += 1 }
            } else if (d.precision <= Decimal.MAX_LONG_DIGITS) {
              val unscaled = v.toUnscaledLong
              var j = 0
              while (j < numRows) { vector.putLong(j, unscaled); j += 1 }
            } else {
              val bytes = v.toJavaBigDecimal.unscaledValue().toByteArray
              var j = 0
              while (j < numRows) { vector.putByteArray(j, bytes); j += 1 }
            }
          case BinaryType =>
            val v = partitionValues.getBinary(i)
            var j = 0
            while (j < numRows) { vector.putByteArray(j, v); j += 1 }
          case _ =>
            // For unsupported types, fill with nulls
            vector.putNulls(0, numRows)
        }
      }
      i += 1
    }
  }
}
