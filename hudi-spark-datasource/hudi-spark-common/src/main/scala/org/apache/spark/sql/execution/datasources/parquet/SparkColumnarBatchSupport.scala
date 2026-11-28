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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.{SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.read.BatchSupport
import org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.sql.vectorized.{RowBatchWriter, RowIdMappedColumnVector}
import org.apache.hudi.storage.StorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

import java.io.Closeable
import java.util

import scala.collection.JavaConverters._

/**
 * The Spark side of the vectorized merge-on-read read of one file slice: base files are read with the vectorized
 * Parquet reader, which appends the slice's partition values to every batch, and output batches carry the requested
 * columns followed by the partition values.
 *
 * @param baseFileReader vectorized reader of the scan, returning batches
 * @param fieldsByName   the Spark type of every column a base read may ask for, by name
 * @param outputSchema   the requested columns followed by the partition columns
 */
private[parquet] class SparkColumnarBatchSupport(baseFileReader: SparkColumnarFileReader,
                                                 file: PartitionedFile,
                                                 fieldsByName: Map[String, StructField],
                                                 state: HoodieFileGroupReadState,
                                                 conf: StorageConfiguration[Configuration],
                                                 partitionValues: InternalRow,
                                                 outputSchema: StructType)
  extends BatchSupport[InternalRow, ColumnarBatch] with SparkAdapterSupport {

  // The vectorized Parquet reader fills the row index into a column it finds missing from the file, which it only
  // accepts for a nullable field
  private lazy val rowIndexField: StructField =
    SparkFileFormatInternalRowReaderContext.getAppliedRequiredSchema(new StructType(), shouldAddRecordPosition = true)
      .fields.head.copy(nullable = true)

  override def readBaseFile(baseFile: HoodieBaseFile, readSchema: HoodieSchema,
                            applyFilters: Boolean): ClosableIterator[ColumnarBatch] = {
    val readStruct = StructType(readSchema.getFields.asScala.map { field =>
      if (field.name() == ROW_INDEX_TEMPORARY_COLUMN_NAME) {
        rowIndexField
      } else {
        fieldsByName.getOrElse(field.name(),
          throw new IllegalStateException(s"Column ${field.name()} is not in the scan's schema"))
      }
    }.toSeq)
    // A reader that returns batches hands them out through its row iterator
    val iter = baseFileReader.read(file, readStruct, state.partitionSchema, state.internalSchemaOpt,
      if (applyFilters) state.filters else Seq.empty, conf, state.tableSchemaAsMessageType).asInstanceOf[Iterator[Any]]
    new ClosableIterator[ColumnarBatch] {
      override def hasNext: Boolean = iter.hasNext

      override def next(): ColumnarBatch = iter.next() match {
        case batch: ColumnarBatch => batch
        case other => throw new IllegalStateException(
          s"Expected columnar batches from the base file of a vectorized merge-on-read read, got ${other.getClass.getName}")
      }

      override def close(): Unit = iter match {
        case closeable: Closeable => closeable.close()
        case _ =>
      }
    }
  }

  override def supportsConcurrentOpen(): Boolean = true

  override def numRows(batch: ColumnarBatch): Int = batch.numRows()

  override def isNullAt(batch: ColumnarBatch, column: Int, row: Int): Boolean = batch.column(column).isNullAt(row)

  override def getLong(batch: ColumnarBatch, column: Int, row: Int): Long = batch.column(column).getLong(row)

  override def getEngineKey(batch: ColumnarBatch, column: Int, row: Int): AnyRef = batch.column(column).getUTF8String(row)

  override def toEngineKey(recordKey: String): AnyRef = UTF8String.fromString(recordKey)

  override def getRow(batch: ColumnarBatch, row: Int): InternalRow = batch.getRow(row)

  override def select(batch: ColumnarBatch, numOutputColumns: Int, numReadColumns: Int, rowIds: Array[Int],
                      numRows: Int): ColumnarBatch = {
    val numPartitionColumns = batch.numCols() - numReadColumns
    val vectors = new Array[ColumnVector](numOutputColumns + numPartitionColumns)
    var i = 0
    while (i < vectors.length) {
      val column = batch.column(if (i < numOutputColumns) i else numReadColumns + i - numOutputColumns)
      vectors(i) = if (rowIds == null) column else new RowIdMappedColumnVector(column, rowIds, numRows)
      i += 1
    }
    sparkAdapter.makeColumnarBatch(vectors, numRows)
  }

  override def toBatch(rows: util.List[InternalRow]): ColumnarBatch = {
    val joined = new util.ArrayList[InternalRow](rows.size())
    rows.asScala.foreach(row => joined.add(new JoinedRow(row, partitionValues)))
    sparkAdapter.makeColumnarBatch(RowBatchWriter.write(outputSchema, joined), rows.size())
  }
}
