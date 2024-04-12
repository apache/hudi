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
import org.apache.hadoop.fs.Path
import org.apache.hudi.SparkFileFormatInternalRowReaderContext.{ROW_INDEX_TEMPORARY_COLUMN_NAME, getAppliedRequiredSchema}
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.collection.{CachingIterator, ClosableIterator, CloseableMappingIterator}
import org.apache.hudi.io.storage.{HoodieSparkFileReaderFactory, HoodieSparkParquetReader}
import org.apache.hudi.util.CloseableInternalRowIterator
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, SparkParquetReader}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import scala.collection.mutable

/**
 * Implementation of {@link HoodieReaderContext} to read {@link InternalRow}s with
 * {@link ParquetFileFormat} on Spark.
 *
 * This uses Spark parquet reader to read parquet data files or parquet log blocks.
 *
 * @param baseFileReader  A reader that transforms a {@link PartitionedFile} to an iterator of
 *                        {@link InternalRow}. This is required for reading the base file and
 *                        not required for reading a file group with only log files.
 * @param partitionValues The values for a partition in which the file group lives.
 */
class SparkFileFormatInternalRowReaderContext(parquetFileReader: SparkParquetReader,
                                              recordKeyColumn: String,
                                              filters: Seq[Filter],
                                              shouldUseRecordPosition: Boolean) extends BaseSparkInternalRowReaderContext {
  lazy val sparkAdapter = SparkAdapterSupport.sparkAdapter
  lazy val sparkFileReaderFactory = new HoodieSparkFileReaderFactory
  val deserializerMap: mutable.Map[Schema, HoodieAvroDeserializer] = mutable.Map()
  lazy val recordKeyFilters: Seq[Filter] = filters.filter(f => f.references.exists(c => c.equalsIgnoreCase(recordKeyColumn)))

  override def getFileRecordIterator(filePath: Path,
                                     start: Long,
                                     length: Long,
                                     dataSchema: Schema,
                                     requiredSchema: Schema,
                                     conf: Configuration): ClosableIterator[InternalRow] = {
    val structType: StructType = HoodieInternalRowUtils.getCachedSchema(requiredSchema)
    if (FSUtils.isLogFile(filePath)) {
      val projection: UnsafeProjection = HoodieInternalRowUtils.getCachedUnsafeProjection(structType, structType)
      new CloseableMappingIterator[InternalRow, UnsafeRow](
        sparkFileReaderFactory.newParquetFileReader(conf, filePath).asInstanceOf[HoodieSparkParquetReader]
          .getInternalRowIterator(dataSchema, requiredSchema),
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
      val (readSchema, readFilters) = getSchemaAndFiltersForRead(structType)
      new CloseableInternalRowIterator(parquetFileReader.read(fileInfo,
        readSchema, StructType(Seq.empty), readFilters, conf))
    }
  }

  private def getSchemaAndFiltersForRead(structType: StructType): (StructType, Seq[Filter]) = {
    (readerState.hasLogFiles.booleanValue(), readerState.needsBootstrapMerge.booleanValue(), shouldUseRecordPosition) match {
      case (false, false, _) =>
        (structType, filters)
      case (false, true, true) if shouldUseRecordPosition =>
        (getAppliedRequiredSchema(structType), filters)
      case (true, _, true) if shouldUseRecordPosition =>
        (getAppliedRequiredSchema(structType), recordKeyFilters)
      case (_, _, _) =>
        (structType, Seq.empty)
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

  override def mergeBootstrapReaders(skeletonFileIterator: ClosableIterator[InternalRow],
                                     skeletonRequiredSchema: Schema,
                                     dataFileIterator: ClosableIterator[InternalRow],
                                     dataRequiredSchema: Schema): ClosableIterator[InternalRow] = {
    doBootstrapMerge(skeletonFileIterator.asInstanceOf[ClosableIterator[Any]], skeletonRequiredSchema,
      dataFileIterator.asInstanceOf[ClosableIterator[Any]], dataRequiredSchema)
  }

  protected def doBootstrapMerge(skeletonFileIterator: ClosableIterator[Any],
                                 skeletonRequiredSchema: Schema,
                                 dataFileIterator: ClosableIterator[Any],
                                 dataRequiredSchema: Schema): ClosableIterator[InternalRow] = {
    if (shouldUseRecordPosition) {
      assert(AvroSchemaUtils.containsFieldInSchema(skeletonRequiredSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME))
      assert(AvroSchemaUtils.containsFieldInSchema(dataRequiredSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME))
      val javaSet = new java.util.HashSet[String]()
      javaSet.add(ROW_INDEX_TEMPORARY_COLUMN_NAME)
      val skeletonProjection = projectRecord(skeletonRequiredSchema,
        AvroSchemaUtils.removeFieldsFromSchema(skeletonRequiredSchema, javaSet))
      //If we have log files, we will want to do position based merging with those as well,
      //so leave a temporary column at the end
      val dataProjection = if (readerState.hasLogFiles) {
        getIdentityProjection
      } else {
        projectRecord(dataRequiredSchema,
          AvroSchemaUtils.removeFieldsFromSchema(dataRequiredSchema, javaSet))
      }

      new CachingIterator[InternalRow] {
        val combinedRow = new JoinedRow()

        private def getPos(row: InternalRow): Long = {
          row.getLong(row.numFields-1)
        }

        private def getNextSkeleton: (InternalRow, Long) = {
          val nextSkeletonRow = skeletonFileIterator.next().asInstanceOf[InternalRow]
          (nextSkeletonRow, getPos(nextSkeletonRow))
        }

        private def getNextData: (InternalRow, Long) = {
          val nextSkeletonRow = skeletonFileIterator.next().asInstanceOf[InternalRow]
          (nextSkeletonRow, getPos(nextSkeletonRow))
        }

        override def close(): Unit = {
          skeletonFileIterator.close()
          dataFileIterator.close()
        }

        override protected def doHasNext(): Boolean = {
          if (!dataFileIterator.hasNext || !skeletonFileIterator.hasNext) {
            false
          } else {
            var nextSkeleton = getNextSkeleton
            var nextData = getNextData
            while (nextSkeleton._2 != nextData._2) {
              if (nextSkeleton._2 > nextData._2) {
                if (!dataFileIterator.hasNext) {
                  return false
                } else {
                  nextData = getNextData
                }
              } else {
                if (!skeletonFileIterator.hasNext) {
                  return false
                } else {
                  nextSkeleton = getNextSkeleton
                }
              }
            }
            nextRecord = combinedRow(skeletonProjection.apply(nextSkeleton._1), dataProjection.apply(nextData._1))
            true
          }
        }
      }
    } else {
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

  override def shouldUseRecordPositionMerging(): Boolean = {
    shouldUseRecordPosition
  }

}

object SparkFileFormatInternalRowReaderContext {
  // From "ParquetFileFormat.scala": The names of the field for record position.
  private val ROW_INDEX = "row_index"
  private val ROW_INDEX_TEMPORARY_COLUMN_NAME = s"_tmp_metadata_$ROW_INDEX"

  // From "namedExpressions.scala": Used to construct to record position field metadata.
  private val FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY = "__file_source_generated_metadata_col"
  private val FILE_SOURCE_METADATA_COL_ATTR_KEY = "__file_source_metadata_col"
  private val METADATA_COL_ATTR_KEY = "__metadata_col"

  def getRecordKeyRelatedFilters(filters: Seq[Filter], recordKeyColumn: String): Seq[Filter] = {
    filters.filter(f => f.references.exists(c => c.equalsIgnoreCase(recordKeyColumn)))
  }

  def hasIndexTempColumn(structType: StructType): Boolean = {
    structType.fields.exists(isIndexTempColumn)
  }

  def isIndexTempColumn(field: StructField): Boolean = {
    field.name.equals(ROW_INDEX_TEMPORARY_COLUMN_NAME)
  }

  def getAppliedRequiredSchema(requiredSchema: StructType): StructType = {
      val metadata = new MetadataBuilder()
        .putString(METADATA_COL_ATTR_KEY, ROW_INDEX_TEMPORARY_COLUMN_NAME)
        .putBoolean(FILE_SOURCE_METADATA_COL_ATTR_KEY, value = true)
        .putString(FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY, ROW_INDEX_TEMPORARY_COLUMN_NAME)
        .build()
      val rowIndexField = StructField(ROW_INDEX_TEMPORARY_COLUMN_NAME, LongType, nullable = false, metadata)
      StructType(requiredSchema.fields.filterNot(isIndexTempColumn) :+ rowIndexField)
  }
}