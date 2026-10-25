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
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.io.memory.HoodieArrowAllocator
import org.apache.hudi.io.storage.VortexRecordIterator
import org.apache.hudi.storage.StorageConfiguration

import dev.vortex.api.{DataSource, ScanOptions, Session}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader, SparkSchemaTransformUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils

import java.io.IOException
import java.net.URI

import scala.collection.JavaConverters._

/**
 * Reader for Vortex files in the Spark datasource query path (vortex-jni 0.76.0). Opens the file
 * via `Session`/`DataSource`, scans it (reading all columns), and converts Arrow batches to Spark
 * rows through [[VortexRecordIterator]], which projects the requested columns by name.
 *
 * Mirrors [[org.apache.spark.sql.execution.datasources.lance.SparkLanceReaderBase]] for the
 * initial Vortex scope: standard Hudi data types, no filter pushdown/data skipping, and no
 * BLOB/VECTOR handling.
 *
 * @param enableVectorizedReader whether to use vectorized reading (currently always true for Vortex)
 */
class SparkVortexReaderBase(enableVectorizedReader: Boolean) extends SparkColumnarFileReader {

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

    val uri = toVortexUri(sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(file).toUri)

    if (requiredSchema.isEmpty && partitionSchema.isEmpty) {
      // No columns requested - return empty iterator
      Iterator.empty
    } else {

      val dataAllocatorSize = storageConf.unwrap().getLong(
        HoodieStorageConfig.VORTEX_READ_ALLOCATOR_SIZE_BYTES.key(),
        HoodieStorageConfig.VORTEX_READ_ALLOCATOR_SIZE_BYTES.defaultValue().toLong)
      val allocator = HoodieArrowAllocator.newChildAllocator(
        getClass.getSimpleName + "-data-" + uri, dataAllocatorSize)

      var vortexIterator: ClosableIterator[UnsafeRow] = null
      var allocatorClosed = false

      try {
        // Session/DataSource are not AutoCloseable in vortex-jni 0.76.0 (native cleanup is implicit).
        val session = Session.create()
        val dataSource = DataSource.open(session, uri)

        // Convert the Vortex file schema to a Spark schema, then reconcile it against the requested
        // schema for schema evolution (implicit type changes, columns not present in the file).
        val fileSchema = ArrowUtils.fromArrowSchema(dataSource.arrowSchema(allocator))
        val (implicitTypeChangeInfo, sparkRequestSchema) =
          SparkSchemaTransformUtils.buildImplicitSchemaChangeInfo(fileSchema, requiredSchema)

        // Only read columns that actually exist in the file; missing columns are null-padded below.
        val requestSchema =
          SparkSchemaTransformUtils.filterSchemaByFileSchema(sparkRequestSchema, fileSchema)

        val baseIter: Iterator[InternalRow] = if (requestSchema.isEmpty) {
          // No data columns to read from the file (only partition columns requested, or all
          // requested columns are new via schema evolution). Emit one empty row per record so the
          // null-padding and partition projections can fill in the values.
          val numRows = dataSource.rowCount().asOptional().orElse(0L)
          allocator.close()
          allocatorClosed = true
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
          val scan = dataSource.scan(ScanOptions.of())
          vortexIterator = new VortexRecordIterator(allocator, session, dataSource, scan, requestSchema, uri)

          // Register cleanup listener
          Option(TaskContext.get()).foreach { ctx =>
            ctx.addTaskCompletionListener[Unit](_ => vortexIterator.close())
          }

          vortexIterator.asScala
        }

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
      } catch {
        case e: Exception =>
          if (vortexIterator != null) {
            vortexIterator.close() // Closes the iterator, which owns the allocator lifecycle
          } else if (!allocatorClosed) {
            allocator.close()
          }
          throw new IOException(s"Failed to read Vortex file: $uri", e)
      }
    }
  }

  /**
   * Vortex requires a well-formed URL. Remote-store paths already carry a scheme
   * (s3://, hdfs://, ...); local paths may not, so qualify them as `file://` URIs. Takes an
   * already-parsed URI (from the storage path) rather than re-parsing a display string, so paths
   * with spaces or other characters needing URI encoding are handled correctly.
   */
  private def toVortexUri(parsed: URI): String = {
    if (parsed.getScheme == null) {
      new java.io.File(parsed.getPath).toURI.toString
    } else {
      parsed.toString
    }
  }
}
