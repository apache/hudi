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

import org.apache.hudi.SparkFileFormatInternalRowReaderContext.{filterIsSafeForBootstrap, getAppliedRequiredSchema}
import org.apache.hudi.avro.{AvroSchemaUtils, HoodieAvroUtils}
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.read.HoodiePositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME
import org.apache.hudi.common.table.read.HoodiePositionBasedSchemaHandler
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.collection.{CachingIterator, ClosableIterator, CloseableMappingIterator}
import org.apache.hudi.io.storage.{HoodieSparkFileReaderFactory, HoodieSparkParquetReader}
import org.apache.hudi.storage.{HoodieStorage, StorageConfiguration, StoragePath}
import org.apache.hudi.util.CloseableInternalRowIterator

import org.apache.avro.Schema
import org.apache.avro.Schema.Type
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.avro.HoodieAvroDeserializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, SparkParquetReader}
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{LongType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.types.UTF8String

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
 * @param requiredFilters   filters that are required and should always be used, even in merging situations
 */
class SparkFileFormatInternalRowReaderContext(parquetFileReader: SparkParquetReader,
                                              recordKeyColumn: String,
                                              filters: Seq[Filter],
                                              requiredFilters: Seq[Filter]) extends BaseSparkInternalRowReaderContext {
  lazy val sparkAdapter: SparkAdapter = SparkAdapterSupport.sparkAdapter
  private lazy val bootstrapSafeFilters: Seq[Filter] = filters.filter(filterIsSafeForBootstrap) ++ requiredFilters
  private val deserializerMap: mutable.Map[Schema, HoodieAvroDeserializer] = mutable.Map()
  private lazy val allFilters = filters ++ requiredFilters

  override def supportsParquetRowIndex: Boolean = {
    HoodieSparkUtils.gteqSpark3_5
  }

  override def getFileRecordIterator(filePath: StoragePath,
                                     start: Long,
                                     length: Long,
                                     dataSchema: Schema,
                                     requiredSchema: Schema,
                                     storage: HoodieStorage): ClosableIterator[InternalRow] = {
    val hasRowIndexField = AvroSchemaUtils.containsFieldInSchema(requiredSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME)
    if (hasRowIndexField) {
      assert(supportsParquetRowIndex())
    }
    val structType = HoodieInternalRowUtils.getCachedSchema(requiredSchema)
    if (FSUtils.isLogFile(filePath)) {
      val dataSchemaWithMergeCol = if (hasRowIndexField) {
        HoodiePositionBasedSchemaHandler.addPositionalMergeCol(dataSchema)
      } else {
        dataSchema
      }
      new CloseableMappingIterator[InternalRow, InternalRow](
        new HoodieSparkFileReaderFactory(storage).newParquetFileReader(filePath)
          .asInstanceOf[HoodieSparkParquetReader].getInternalRowIterator(dataSchema, dataSchemaWithMergeCol),
        projectRecord(dataSchemaWithMergeCol, requiredSchema))
    } else {
      // partition value is empty because the spark parquet reader will append the partition columns to
      // each row if they are given. That is the only usage of the partition values in the reader.
      val fileInfo = sparkAdapter.getSparkPartitionedFileUtils
        .createPartitionedFile(InternalRow.empty, filePath, start, length)
      val (readSchema, readFilters) = getSchemaAndFiltersForRead(structType, hasRowIndexField)
      new CloseableInternalRowIterator(parquetFileReader.read(fileInfo,
        readSchema, StructType(Seq.empty), getSchemaHandler.getInternalSchemaOpt,
        readFilters, storage.getConf.asInstanceOf[StorageConfiguration[Configuration]]))
    }
  }

  private def getSchemaAndFiltersForRead(structType: StructType, hasRowIndexField: Boolean): (StructType, Seq[Filter]) = {
    val schemaForRead = getAppliedRequiredSchema(structType, hasRowIndexField)
    if (!getHasLogFiles && !getNeedsBootstrapMerge) {
      (schemaForRead, allFilters)
    } else if (!getHasLogFiles && hasRowIndexField) {
      (schemaForRead, bootstrapSafeFilters)
    } else {
      (schemaForRead, requiredFilters)
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
    doBootstrapMerge(skeletonFileIterator.asInstanceOf[ClosableIterator[Any]], skeletonRequiredSchema,
      dataFileIterator.asInstanceOf[ClosableIterator[Any]], dataRequiredSchema)
  }

  private def doBootstrapMerge(skeletonFileIterator: ClosableIterator[Any],
                               skeletonRequiredSchema: Schema,
                               dataFileIterator: ClosableIterator[Any],
                               dataRequiredSchema: Schema): ClosableIterator[InternalRow] = {
    if (supportsParquetRowIndex()) {
      assert(AvroSchemaUtils.containsFieldInSchema(skeletonRequiredSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME))
      assert(AvroSchemaUtils.containsFieldInSchema(dataRequiredSchema, ROW_INDEX_TEMPORARY_COLUMN_NAME))
      val rowIndexColumn = new java.util.HashSet[String]()
      rowIndexColumn.add(ROW_INDEX_TEMPORARY_COLUMN_NAME)
      //always remove the row index column from the skeleton because the data file will also have the same column
      val skeletonProjection = projectRecord(skeletonRequiredSchema,
        HoodieAvroUtils.removeFields(skeletonRequiredSchema, rowIndexColumn))

      //If we need to do position based merging with log files we will leave the row index column at the end
      val dataProjection = if (getHasLogFiles && getShouldMergeUseRecordPosition) {
        getIdentityProjection
      } else {
        projectRecord(dataRequiredSchema,
          HoodieAvroUtils.removeFields(dataRequiredSchema, rowIndexColumn))
      }

      //row index will always be the last column
      val skeletonRowIndex = skeletonRequiredSchema.getFields.size() - 1
      val dataRowIndex = dataRequiredSchema.getFields.size() - 1

      //Always use internal row for positional merge because
      //we need to iterate row by row when merging
      new CachingIterator[InternalRow] {
        val combinedRow = new JoinedRow()

        private def getNextSkeleton: (InternalRow, Long) = {
          val nextSkeletonRow = skeletonFileIterator.next().asInstanceOf[InternalRow]
          (nextSkeletonRow, nextSkeletonRow.getLong(skeletonRowIndex))
        }

        private def getNextData: (InternalRow, Long) = {
          val nextDataRow = dataFileIterator.next().asInstanceOf[InternalRow]
          (nextDataRow,  nextDataRow.getLong(dataRowIndex))
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
              //This will not be used until [HUDI-7693] is implemented
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

  override def castValue(value: Comparable[_], newType: Schema.Type): Comparable[_] = {
    value match {
      case v: Integer => newType match {
        case Type.INT => v
        case Type.LONG => v.longValue()
        case Type.FLOAT => v.floatValue()
        case Type.DOUBLE => v.doubleValue()
        case Type.STRING => UTF8String.fromString(v.toString)
        case x => throw new UnsupportedOperationException(s"Cast from Integer to $x is not supported")
      }
      case v: java.lang.Long => newType match {
        case Type.LONG => v
        case Type.FLOAT => v.floatValue()
        case Type.DOUBLE => v.doubleValue()
        case Type.STRING => UTF8String.fromString(v.toString)
        case x => throw new UnsupportedOperationException(s"Cast from Long to $x is not supported")
      }
      case v: java.lang.Float => newType match {
        case Type.FLOAT => v
        case Type.DOUBLE => v.doubleValue()
        case Type.STRING => UTF8String.fromString(v.toString)
        case x => throw new UnsupportedOperationException(s"Cast from Float to $x is not supported")
      }
      case v: java.lang.Double => newType match {
        case Type.DOUBLE => v
        case Type.STRING => UTF8String.fromString(v.toString)
        case x => throw new UnsupportedOperationException(s"Cast from Double to $x is not supported")
      }
      case v: String => newType match {
        case Type.STRING => UTF8String.fromString(v)
        case x => throw new UnsupportedOperationException(s"Cast from String to $x is not supported")
      }
      case v: UTF8String => newType match {
        case Type.STRING => v
        case x => throw new UnsupportedOperationException(s"Cast from String to $x is not supported")
      }
    }
  }
}

object SparkFileFormatInternalRowReaderContext {
  // From "namedExpressions.scala": Used to construct to record position field metadata.
  private val FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY = "__file_source_generated_metadata_col"
  private val FILE_SOURCE_METADATA_COL_ATTR_KEY = "__file_source_metadata_col"
  private val METADATA_COL_ATTR_KEY = "__metadata_col"

  def getAppliedRequiredSchema(requiredSchema: StructType, shouldAddRecordPosition: Boolean): StructType = {
    if (shouldAddRecordPosition) {
      val metadata = new MetadataBuilder()
        .putString(METADATA_COL_ATTR_KEY, ROW_INDEX_TEMPORARY_COLUMN_NAME)
        .putBoolean(FILE_SOURCE_METADATA_COL_ATTR_KEY, value = true)
        .putString(FILE_SOURCE_GENERATED_METADATA_COL_ATTR_KEY, ROW_INDEX_TEMPORARY_COLUMN_NAME)
        .build()
      val rowIndexField = StructField(ROW_INDEX_TEMPORARY_COLUMN_NAME, LongType, nullable = false, metadata)
      StructType(requiredSchema.fields.filterNot(isIndexTempColumn) :+ rowIndexField)
    } else {
      requiredSchema
    }
  }

  /**
   * Only valid if there is support for RowIndexField and no log files
   * Filters are safe for bootstrap if meta col filters are independent from data col filters.
   */
  def filterIsSafeForBootstrap(filter: Filter): Boolean = {
    val metaRefCount = filter.references.count(c => HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(c.toLowerCase))
    metaRefCount == filter.references.length || metaRefCount == 0
  }

  private def isIndexTempColumn(field: StructField): Boolean = {
    field.name.equals(ROW_INDEX_TEMPORARY_COLUMN_NAME)
  }

}