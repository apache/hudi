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

package org.apache.hudi

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.collection.{ClosableIterator, CloseableMappingIterator}
import org.apache.hudi.io.storage.{HoodieSparkFileReaderFactory, HoodieSparkParquetReader}
import org.apache.hudi.storage.{HoodieStorage, StorageConfiguration, StoragePath}
import org.apache.hudi.util.CloseableInternalRowIterator
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeRow}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, SparkParquetReader}
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.mutable

/**
 * Implementation of [[HoodieReaderContext]] to read [[InternalRow]]s with
 * [[ParquetFileFormat]] on Spark.
 *
 * This uses Spark parquet reader to read parquet data files or parquet log blocks.
 *
 * @param parquetFileReader A reader that transforms a [[PartitionedFile]] to an iterator of
 *                          [[InternalRow]]. This is required for reading the base file and
 *                          not required for reading a file group with only log files.
 * @param recordKeyColumn   column name for the recordkey
 * @param filters           spark filters that might be pushed down into the reader
 */
class SparkFileFormatInternalRowReaderContext(parquetFileReader: SparkParquetReader,
                                              recordKeyColumn: String,
                                              filters: Seq[Filter]) extends BaseSparkInternalRowReaderContext {
  lazy val sparkAdapter: SparkAdapter = SparkAdapterSupport.sparkAdapter
  private val deserializerMap: mutable.Map[Schema, HoodieAvroDeserializer] = mutable.Map()

  override def getFileRecordIterator(filePath: StoragePath,
                                     start: Long,
                                     length: Long,
                                     dataSchema: Schema,
                                     requiredSchema: Schema,
                                     storage: HoodieStorage): ClosableIterator[InternalRow] = {
    val structType = HoodieInternalRowUtils.getCachedSchema(requiredSchema)
    if (FSUtils.isLogFile(filePath)) {
      val projection = HoodieInternalRowUtils.getCachedUnsafeProjection(structType, structType)
      new CloseableMappingIterator[InternalRow, UnsafeRow](
        new HoodieSparkFileReaderFactory(storage).newParquetFileReader(filePath)
          .asInstanceOf[HoodieSparkParquetReader].getInternalRowIterator(dataSchema, requiredSchema),
        new java.util.function.Function[InternalRow, UnsafeRow] {
          override def apply(data: InternalRow): UnsafeRow = {
            // NOTE: We have to do [[UnsafeProjection]] of incoming [[InternalRow]] to convert
            //       it to [[UnsafeRow]] holding just raw bytes
            projection.apply(data)
          }
        }).asInstanceOf[ClosableIterator[InternalRow]]
    } else {
      // partition value is empty because the spark parquet reader will append the partition columns to
      // each row if they are given. That is the only usage of the partition values in the reader.
      val fileInfo = sparkAdapter.getSparkPartitionedFileUtils
        .createPartitionedFile(InternalRow.empty, filePath, start, length)
      new CloseableInternalRowIterator(parquetFileReader.read(fileInfo,
        structType, StructType(Seq.empty), Seq.empty, storage.getConf.asInstanceOf[StorageConfiguration[Configuration]]))
    }
  }

  /**
   * Converts an Avro record, e.g., serialized in the log files, to an [[InternalRow]].
   *
   * @param avroRecord The Avro record.
   * @return An [[InternalRow]].
   */
  override def convertAvroRecord(avroRecord: IndexedRecord): InternalRow = {
    val schema = avroRecord.getSchema
    val structType = HoodieInternalRowUtils.getCachedSchema(schema)
    val deserializer = deserializerMap.getOrElseUpdate(schema, {
      sparkAdapter.createAvroDeserializer(schema, structType)
    })
    deserializer.deserialize(avroRecord).get.asInstanceOf[InternalRow]
  }

  /**
   * Merge the skeleton file and data file iterators into a single iterator that will produce rows that contain all columns from the
   * skeleton file iterator, followed by all columns in the data file iterator
   *
   * @param skeletonFileIterator iterator over bootstrap skeleton files that contain hudi metadata columns
   * @param dataFileIterator     iterator over data files that were bootstrapped into the hudi table
   * @return iterator that concatenates the skeletonFileIterator and dataFileIterator
   */
  override def mergeBootstrapReaders(skeletonFileIterator: ClosableIterator[InternalRow],
                                     skeletonRequiredSchema: Schema,
                                     dataFileIterator: ClosableIterator[InternalRow],
                                     dataRequiredSchema: Schema): ClosableIterator[InternalRow] = {
    doBootstrapMerge(skeletonFileIterator.asInstanceOf[ClosableIterator[Any]],
      dataFileIterator.asInstanceOf[ClosableIterator[Any]])
  }

  protected def doBootstrapMerge(skeletonFileIterator: ClosableIterator[Any], dataFileIterator: ClosableIterator[Any]): ClosableIterator[InternalRow] = {
    new ClosableIterator[Any] {
      val combinedRow = new JoinedRow()

      override def hasNext: Boolean = {
        //If the iterators are out of sync it is probably due to filter pushdown
        checkState(dataFileIterator.hasNext == skeletonFileIterator.hasNext,
          "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!")
        dataFileIterator.hasNext && skeletonFileIterator.hasNext
      }

      override def next(): Any = {
        (skeletonFileIterator.next(), dataFileIterator.next()) match {
          case (s: ColumnarBatch, d: ColumnarBatch) =>
            val numCols = s.numCols() + d.numCols()
            val vecs: Array[ColumnVector] = new Array[ColumnVector](numCols)
            for (i <- 0 until numCols) {
              if (i < s.numCols()) {
                vecs(i) = s.column(i)
              } else {
                vecs(i) = d.column(i - s.numCols())
              }
            }
            assert(s.numRows() == d.numRows())
            sparkAdapter.makeColumnarBatch(vecs, s.numRows())
          case (_: ColumnarBatch, _: InternalRow) => throw new IllegalStateException("InternalRow ColumnVector mismatch")
          case (_: InternalRow, _: ColumnarBatch) => throw new IllegalStateException("InternalRow ColumnVector mismatch")
          case (s: InternalRow, d: InternalRow) => combinedRow(s, d)
        }
      }

      override def close(): Unit = {
        skeletonFileIterator.close()
        dataFileIterator.close()
      }
    }.asInstanceOf[ClosableIterator[InternalRow]]
  }
}
