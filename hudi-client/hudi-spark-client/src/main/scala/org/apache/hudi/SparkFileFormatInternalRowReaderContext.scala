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

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.SparkFileFormatInternalRowReaderContext.{filterIsSafeForBootstrap, filterIsSafeForPrimaryKey, getAppliedRequiredSchema}
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieRecord}
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaUtils}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.read.buffer.PositionBasedFileGroupRecordBuffer.ROW_INDEX_TEMPORARY_COLUMN_NAME
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.collection.{CachingIterator, ClosableIterator, Pair => HPair}
import org.apache.hudi.io.storage.{HoodieSparkFileReaderFactory, HoodieSparkParquetReader, VectorConversionUtils}
import org.apache.hudi.storage.{HoodieStorage, StorageConfiguration, StoragePath}
import org.apache.hudi.util.CloseableInternalRowIterator
import org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.hudi.SparkAdapter
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{ArrayType, ByteType, DoubleType, FloatType, LongType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

import java.util.function.{Function => JFunction}

import scala.collection.JavaConverters._

/**
 * Implementation of [[HoodieReaderContext]] to read [[InternalRow]]s with
 * [[ParquetFileFormat]] on Spark.
 *
 * This uses Spark parquet reader to read parquet data files or parquet log blocks.
 *
 * @param parquetFileReader A reader that transforms a [[PartitionedFile]] to an iterator of
 *                          [[InternalRow]]. This is required for reading the base file and
 *                          not required for reading a file group with only log files.
 * @param filters           spark filters that might be pushed down into the reader
 * @param requiredFilters   filters that are required and should always be used, even in merging situations
 */
class SparkFileFormatInternalRowReaderContext(baseFileReader: SparkColumnarFileReader,
                                              filters: Seq[Filter],
                                              requiredFilters: Seq[Filter],
                                              storageConfiguration: StorageConfiguration[_],
                                              tableConfig: HoodieTableConfig,
                                              sparkDataSchema: Option[StructType] = None,
                                              sparkRequiredSchema: Option[StructType] = None)
  extends BaseSparkInternalRowReaderContext(storageConfiguration, tableConfig, SparkFileFormatInternalRecordContext.apply(tableConfig)) {

  // Java-friendly auxiliary constructor (Scala default args don't generate matching Java overloads).
  def this(baseFileReader: SparkColumnarFileReader,
           filters: Seq[Filter],
           requiredFilters: Seq[Filter],
           storageConfiguration: StorageConfiguration[_],
           tableConfig: HoodieTableConfig) =
    this(baseFileReader, filters, requiredFilters, storageConfiguration, tableConfig, None, None)

  lazy val sparkAdapter: SparkAdapter = SparkAdapterSupport.sparkAdapter
  private lazy val recordKeyFields = Option(tableConfig.getRecordKeyFields.orElse(null)).map(_.map(_.toLowerCase).toSet).getOrElse(Set.empty)
  private lazy val bootstrapSafeFilters: Seq[Filter] = filters.filter(filterIsSafeForBootstrap) ++ requiredFilters
  private lazy val morFilters = filters.filter(filterIsSafeForPrimaryKey(_, recordKeyFields)) ++ requiredFilters
  private lazy val allFilters = filters ++ requiredFilters

  // Engine-neutral hook used by FileGroupRecordBuffer to align log-block records with the
  // PushVariantIntoScan-projected variant shape before they reach the merger.
  //
  // The data block carries v as a real VariantType plus Hudi's merger metadata columns
  // (_hoodie_record_key for key-based merge, _tmp_metadata_row_index for position-based).
  // The downstream merger reads those metadata columns by ordinal to look records up. So the
  // projector must preserve every field of the data-block schema and only rewrite variant
  // fields into their PushVariantIntoScan struct shape — projecting down to the bare required
  // schema (id,name,v,ts) drops _hoodie_record_key / _tmp_metadata_row_index and the merger
  // then reads garbage offsets, surfacing as a SIGBUS-translated InternalError in
  // FileScanRDD.hasNext during the next shuffle's RangePartitioner sample.
  override def getLogBlockRecordProjection(
      dataBlockSchema: HoodieSchema): HOption[JFunction[InternalRow, InternalRow]] = {
    val needsProjection = sparkRequiredSchema.exists(_.fields.exists(f => f.dataType match {
      case st: StructType => sparkAdapter.isVariantProjectionStruct(st)
      case _ => false
    }))
    if (!needsProjection) {
      return HOption.empty[JFunction[InternalRow, InternalRow]]()
    }
    val req = sparkRequiredSchema.get
    val dataStruct = HoodieInternalRowUtils.getCachedSchema(dataBlockSchema)
    // Build the projector's target schema from dataStruct, replacing each variant field with
    // the PushVariantIntoScan-projected struct shape from the required schema. Non-variant
    // fields (including merger metadata cols absent from the required schema) pass through.
    val targetFields = dataStruct.fields.map { df =>
      SparkFileFormatInternalRowReaderContext.findFieldByName(req, df.name).map(_.dataType) match {
        case Some(projStruct: StructType) if sparkAdapter.isVariantProjectionStruct(projStruct) =>
          df.copy(dataType = projStruct)
        case _ => df
      }
    }
    val targetStruct = StructType(targetFields)
    sparkAdapter.buildVariantProjector(dataStruct, targetStruct) match {
      case Some(p) => HOption.of(new JFunction[InternalRow, InternalRow] {
        // buildVariantProjector returns an UnsafeProjection whose `apply` reuses a single
        // UnsafeRow buffer. The merge path stores these rows into ExternalSpillableMap, so we
        // must copy before the next invocation overwrites the buffer.
        override def apply(r: InternalRow): InternalRow = p(r).copy()
      })
      case None => HOption.empty[JFunction[InternalRow, InternalRow]]()
    }
  }

  override def getFileRecordIterator(filePath: StoragePath,
                                     start: Long,
                                     length: Long,
                                     dataSchema: HoodieSchema, // dataSchema refers to table schema in most cases(non log file reads).
                                     requiredSchema: HoodieSchema,
                                     storage: HoodieStorage): ClosableIterator[InternalRow] = {
    val hasRowIndexField = requiredSchema.getField(ROW_INDEX_TEMPORARY_COLUMN_NAME).isPresent
    if (hasRowIndexField) {
      assert(getRecordContext.supportsParquetRowIndex())
    }
    // The function-arg requiredSchema is the engine's *augmented* required schema — Hudi's
    // schema handler has already added the merger's metadata columns (_hoodie_record_key for
    // key-based merge, _tmp_metadata_row_index for position-based) to the user-requested
    // fields. Start from that so the parquet read includes those columns (the merger needs
    // them to extract record keys / positions from base rows).
    //
    // Then overlay variant projection metadata from sparkRequiredSchema: when Spark 4.1's
    // PushVariantIntoScan rewrote a variant column into struct<0:..> with VariantMetadata,
    // parquet-mr can do that projection natively — but only if the field carries the
    // VariantMetadata. The augmented HoodieSchema-derived struct loses that metadata
    // (HoodieSparkSchemaConverters collapses the projected struct back to VariantType), so
    // we copy the projected struct shape from sparkRequiredSchema where it exists.
    val structType = sparkRequiredSchema match {
      case Some(sparkReq) =>
        val augmentedStruct = HoodieInternalRowUtils.getCachedSchema(requiredSchema)
        StructType(augmentedStruct.fields.map { f =>
          SparkFileFormatInternalRowReaderContext.findFieldByName(sparkReq, f.name).map(_.dataType) match {
            case Some(projStruct: StructType) if sparkAdapter.isVariantProjectionStruct(projStruct) =>
              f.copy(dataType = projStruct)
            case _ => f
          }
        })
      case None => HoodieInternalRowUtils.getCachedSchema(requiredSchema)
    }

    // Parquet stores VECTOR as FIXED_LEN_BYTE_ARRAY, so the reader needs BinaryType
    // and we decode back to ArrayType below. Lance returns ArrayType natively, so skip
    // the rewrite only for Lance base files; log files always go through the rewrite path.
    val isLanceBaseFile = FSUtils.isBaseFile(filePath) &&
      tableConfig.getBaseFileFormat == HoodieFileFormat.LANCE
    val vectorColumnInfo: Map[Int, HoodieSchema.Vector] = if (isLanceBaseFile) {
      Map.empty
    } else {
      SparkFileFormatInternalRowReaderContext.detectVectorColumns(requiredSchema)
    }
    val parquetReadStructType = if (vectorColumnInfo.nonEmpty) {
      SparkFileFormatInternalRowReaderContext.replaceVectorColumnsWithBinary(structType, vectorColumnInfo)
    } else {
      structType
    }

    val (readSchema, readFilters) = getSchemaAndFiltersForRead(parquetReadStructType, hasRowIndexField)
    if (FSUtils.isLogFile(filePath)) {
      // NOTE: now only primary key based filtering is supported for log files
      // Records come out with v as VARIANT (the parquet log block's native shape). Variant
      // alignment to PushVariantIntoScan's projected struct shape happens later in the
      // FileGroupRecordBuffer via getLogBlockRecordProjection.
      new HoodieSparkFileReaderFactory(storage).newParquetFileReader(filePath)
        .asInstanceOf[HoodieSparkParquetReader].getUnsafeRowIterator(requiredSchema, readFilters.asJava)
        .asInstanceOf[ClosableIterator[InternalRow]]
    } else {
      // partition value is empty because the spark parquet reader will append the partition columns to
      // each row if they are given. That is the only usage of the partition values in the reader.
      val fileInfo = sparkAdapter.getSparkPartitionedFileUtils
        .createPartitionedFile(InternalRow.empty, filePath, start, length)

      // Convert Avro dataSchema to Parquet MessageType for timestamp precision conversion
      val tableSchemaOpt = if (dataSchema != null) {
        val hadoopConf = storage.getConf.unwrapAs(classOf[Configuration])
        val parquetSchema = getAvroSchemaConverter(hadoopConf).convert(dataSchema)
        org.apache.hudi.common.util.Option.of(parquetSchema)
      } else {
        org.apache.hudi.common.util.Option.empty[org.apache.parquet.schema.MessageType]()
      }
      val rawIterator = new CloseableInternalRowIterator(baseFileReader.read(fileInfo,
        readSchema, StructType(Seq.empty), getSchemaHandler.getInternalSchemaOpt,
        readFilters, storage.getConf.asInstanceOf[StorageConfiguration[Configuration]], tableSchemaOpt))

      // Post-process: convert binary VECTOR columns back to typed arrays
      if (vectorColumnInfo.nonEmpty) {
        SparkFileFormatInternalRowReaderContext.wrapWithVectorConversion(rawIterator, vectorColumnInfo, readSchema)
      } else {
        rawIterator
      }
    }
  }

  private def getSchemaAndFiltersForRead(structType: StructType, hasRowIndexField: Boolean): (StructType, Seq[Filter]) = {
    val schemaForRead = getAppliedRequiredSchema(structType, hasRowIndexField)
    if (!getHasLogFiles && !getNeedsBootstrapMerge) {
      (schemaForRead, allFilters)
    } else if (!getHasLogFiles && hasRowIndexField) {
      (schemaForRead, bootstrapSafeFilters)
    } else if (!getNeedsBootstrapMerge) {
      (schemaForRead, morFilters)
    } else {
      (schemaForRead, requiredFilters)
    }
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
                                     skeletonRequiredSchema: HoodieSchema,
                                     dataFileIterator: ClosableIterator[InternalRow],
                                     dataRequiredSchema: HoodieSchema,
                                     partitionFieldAndValues: java.util.List[HPair[String, Object]]): ClosableIterator[InternalRow] = {
    doBootstrapMerge(skeletonFileIterator.asInstanceOf[ClosableIterator[Any]], skeletonRequiredSchema,
      dataFileIterator.asInstanceOf[ClosableIterator[Any]], dataRequiredSchema, partitionFieldAndValues)
  }

  private def doBootstrapMerge(skeletonFileIterator: ClosableIterator[Any],
                               skeletonRequiredSchema: HoodieSchema,
                               dataFileIterator: ClosableIterator[Any],
                               dataRequiredSchema: HoodieSchema,
                               partitionFieldAndValues: java.util.List[HPair[String, Object]]): ClosableIterator[InternalRow] = {
    if (getRecordContext.supportsParquetRowIndex()) {
      assert(skeletonRequiredSchema.getField(ROW_INDEX_TEMPORARY_COLUMN_NAME).isPresent)
      assert(dataRequiredSchema.getField(ROW_INDEX_TEMPORARY_COLUMN_NAME).isPresent)
      val rowIndexColumn = new java.util.HashSet[String]()
      rowIndexColumn.add(ROW_INDEX_TEMPORARY_COLUMN_NAME)
      //always remove the row index column from the skeleton because the data file will also have the same column
      val skeletonProjection = recordContext.projectRecord(skeletonRequiredSchema,
        HoodieSchemaUtils.removeFields(skeletonRequiredSchema, rowIndexColumn))

      //If we need to do position based merging with log files we will leave the row index column at the end
      val dataProjection = if (getShouldMergeUseRecordPosition) {
        getBootstrapProjection(dataRequiredSchema, dataRequiredSchema, partitionFieldAndValues)
      } else {
        getBootstrapProjection(dataRequiredSchema,
          HoodieSchemaUtils.removeFields(dataRequiredSchema, rowIndexColumn), partitionFieldAndValues)
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
      val dataProjection = getBootstrapProjection(dataRequiredSchema, dataRequiredSchema, partitionFieldAndValues)
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
            case (s: InternalRow, d: InternalRow) => combinedRow(s, dataProjection.apply(d))
          }
        }

        override def close(): Unit = {
          skeletonFileIterator.close()
          dataFileIterator.close()
        }
      }.asInstanceOf[ClosableIterator[InternalRow]]
    }
  }
}

object SparkFileFormatInternalRowReaderContext {
  /**
   * Look up a field by name on a Spark StructType, honoring the session's
   * `spark.sql.caseSensitive` setting. Used when overlaying variant projection metadata from
   * the Spark required schema onto the Hudi-derived data schema, where the two schemas may
   * have arrived through different name-resolution paths.
   */
  private[hudi] def findFieldByName(schema: StructType, name: String): Option[StructField] = {
    if (SQLConf.get.caseSensitiveAnalysis) {
      schema.fields.find(_.name == name)
    } else {
      schema.fields.find(_.name.equalsIgnoreCase(name))
    }
  }

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

  /**
   * Only valid if the filter's references only include primary key columns or {@link HoodieRecord.RECORD_KEY_METADATA_FIELD}
   */
  def filterIsSafeForPrimaryKey(filter: Filter, recordKeyFields: Set[String]): Boolean = {
    filter.references.forall(c => recordKeyFields.contains(c.toLowerCase) || c.equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD))
  }

  private def isIndexTempColumn(field: StructField): Boolean = {
    field.name.equals(ROW_INDEX_TEMPORARY_COLUMN_NAME)
  }

  /**
   * Detects VECTOR columns from HoodieSchema.
   * Delegates to [[VectorConversionUtils.detectVectorColumns]].
   * @return Map of ordinal to Vector schema for VECTOR fields.
   */
  private[hudi] def detectVectorColumns(schema: HoodieSchema): Map[Int, HoodieSchema.Vector] = {
    VectorConversionUtils.detectVectorColumns(schema).asScala.map { case (k, v) => (k.intValue(), v) }.toMap
  }

  /**
   * Detects VECTOR columns from Spark StructType metadata.
   * Delegates to [[VectorConversionUtils.detectVectorColumnsFromMetadata]].
   * @return Map of ordinal to Vector schema for VECTOR fields.
   */
  def detectVectorColumnsFromMetadata(schema: StructType): Map[Int, HoodieSchema.Vector] = {
    VectorConversionUtils.detectVectorColumnsFromMetadata(schema).asScala.map { case (k, v) => (k.intValue(), v) }.toMap
  }

  /**
   * Replaces ArrayType with BinaryType for VECTOR columns so the Parquet reader
   * can read FIXED_LEN_BYTE_ARRAY data without type mismatch.
   * Delegates to [[VectorConversionUtils.replaceVectorColumnsWithBinary]].
   */
  def replaceVectorColumnsWithBinary(structType: StructType, vectorColumns: Map[Int, HoodieSchema.Vector]): StructType = {
    val javaMap = vectorColumns.map { case (k, v) => (Integer.valueOf(k), v.asInstanceOf[AnyRef]) }.asJava
    VectorConversionUtils.replaceVectorColumnsWithBinary(structType, javaMap)
  }

  /**
   * Wraps an iterator to convert binary VECTOR columns back to typed arrays.
   * Unpacks bytes from FIXED_LEN_BYTE_ARRAY into GenericArrayData using the canonical vector byte order.
   * Uses UnsafeProjection to make a defensive copy of each row.
   */
  private[hudi] def wrapWithVectorConversion(
      iterator: ClosableIterator[InternalRow],
      vectorColumns: Map[Int, HoodieSchema.Vector],
      readSchema: StructType): ClosableIterator[InternalRow] = {
    val javaVectorCols: java.util.Map[Integer, HoodieSchema.Vector] =
      vectorColumns.map { case (k, v) => (Integer.valueOf(k), v) }.asJava
    // Build output schema: replace BinaryType with the correct ArrayType for vector columns
    val outputFields = readSchema.fields.zipWithIndex.map { case (field, i) =>
      vectorColumns.get(i) match {
        case Some(vec) =>
          val elemType = vec.getVectorElementType match {
            case HoodieSchema.Vector.VectorElementType.FLOAT => FloatType
            case HoodieSchema.Vector.VectorElementType.DOUBLE => DoubleType
            case HoodieSchema.Vector.VectorElementType.INT8 => ByteType
          }
          field.copy(dataType = ArrayType(elemType, containsNull = false))
        case None => field
      }
    }
    val outputSchema = StructType(outputFields)
    val projection = UnsafeProjection.create(outputSchema)
    val mapper = VectorConversionUtils.buildRowMapper(readSchema, javaVectorCols, projection.apply(_))
    new ClosableIterator[InternalRow] {
      override def hasNext: Boolean = iterator.hasNext
      override def next(): InternalRow = mapper.apply(iterator.next())
      override def close(): Unit = iterator.close()
    }
  }

}
