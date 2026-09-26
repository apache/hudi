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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.{HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, HoodieTableSchema, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.cdc.{CDCFileGroupIterator, HoodieCDCFileGroupSplit, HoodieCDCFileIndex}
import org.apache.hudi.common.config.{HoodieReaderConfig, TypedProperties}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.schema.internal.InternalSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, ParquetTableSchemaResolver}
import org.apache.hudi.common.table.log.InstantRange
import org.apache.hudi.common.table.read.{HoodieFileGroupReader, HoodieRecordReader}
import org.apache.hudi.common.table.read.lsm.{HoodieLsmFileGroupReader, LsmReaderUtils}
import org.apache.hudi.common.util.{ConfigUtils, Option => HOption}
import org.apache.hudi.common.util.collection.ClosableIterator
import org.apache.hudi.data.CloseableIteratorListener
import org.apache.hudi.io.storage.HoodieSparkParquetReader.ENABLE_LOGICAL_TIMESTAMP_REPAIR
import org.apache.hudi.io.storage.VectorConversionUtils
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.schema.MessageType
import org.apache.spark.SparkEnv
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{JoinedRow, UnsafeProjection}
import org.apache.spark.sql.execution.datasources.{PartitionedFile, SparkColumnarFileReader, SparkSchemaTransformUtils}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchUtils}
import org.apache.spark.util.{SerializableConfiguration, Utils}

import java.io.Closeable
import java.nio.ByteBuffer

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.reflect.ClassTag

/**
 * Read-only state of one scan of [[HoodieFileGroupReaderBasedFileFormat]], built on the driver and shared by every
 * task of an executor through [[HoodieFileGroupReaderFunction]]. Executors only fill thread-safe lazy caches in it;
 * per-file state such as reader properties is copied before use.
 */
private[parquet] class HoodieFileGroupReadState(val metaClient: HoodieTableMetaClient,
                                                 val tableSchema: HoodieTableSchema,
                                                 val queryTimestamp: String,
                                                 val readerProps: TypedProperties,
                                                 val cdcProps: TypedProperties,
                                                 val dataSchema: HoodieSchema,
                                                 val requestedSchema: HoodieSchema,
                                                 val internalSchemaOpt: HOption[InternalSchema],
                                                 val instantRangeOpt: HOption[InstantRange],
                                                 val shouldUseRecordPosition: Boolean,
                                                 val isCount: Boolean,
                                                 val filters: Seq[Filter],
                                                 val requiredFilters: Seq[Filter],
                                                 val requiredSchema: StructType,
                                                 val partitionSchema: StructType,
                                                 val remainingPartitionSchema: StructType,
                                                 val fixedPartitionIndexes: Set[Int],
                                                 val outputSchema: StructType,
                                                 val projectionInputSchema: StructType,
                                                 val baseFileReadSchemas: BaseFileReadSchemas) extends Serializable {

  /**
   * Parquet form of the table schema for base file reads. The conversion reads Hadoop's default resources, so it is
   * done once per executor rather than per task or file.
   */
  @transient lazy val tableSchemaAsMessageType: HOption[MessageType] =
    HOption.ofNullable(ParquetTableSchemaResolver.convertAvroSchemaToParquet(tableSchema.schema, new Configuration()))
}

/**
 * Base file read schemas with VECTOR columns rewritten to BinaryType, plus the ordinals of those columns.
 */
private[parquet] case class BaseFileReadSchemas(readRequiredSchema: StructType,
                                                readVectorColumns: Map[Int, HoodieSchema.Vector],
                                                outputSchema: StructType,
                                                outputVectorColumns: Map[Int, HoodieSchema.Vector],
                                                requestedSchema: StructType)

/**
 * Holds a value as Java-serialized bytes and deserializes it at most once per JVM instance of the holder.
 *
 * <p>Broadcasting this holder instead of the value keeps the broadcast independent of `spark.serializer`: Kryo
 * would otherwise serialize the value field by field and ignore the custom Java serialization of types such as
 * [[HoodieSchema]] and [[HadoopStorageConfiguration]].
 */
private[parquet] class JavaSerializedValue[T: ClassTag] private(bytes: Array[Byte]) extends Serializable {

  @transient lazy val value: T = SparkEnv.get.closureSerializer.newInstance()
    .deserialize[T](ByteBuffer.wrap(bytes), Utils.getContextOrSparkClassLoader)
}

private[parquet] object JavaSerializedValue {

  def apply[T: ClassTag](value: T): JavaSerializedValue[T] = {
    val buffer = SparkEnv.get.closureSerializer.newInstance().serialize(value)
    val bytes = new Array[Byte](buffer.remaining())
    buffer.get(bytes)
    new JavaSerializedValue[T](bytes)
  }
}

/**
 * The per-file read function returned by [[HoodieFileGroupReaderBasedFileFormat.buildReaderWithPartitionValues]].
 *
 * <p>Spark deserializes this function once per task, so it holds only broadcast handles. The scan state behind
 * `state` is deserialized once per executor.
 */
private[parquet] class HoodieFileGroupReaderFunction(baseFileReader: Broadcast[SparkColumnarFileReader],
                                                     fileGroupBaseFileReader: Broadcast[SparkColumnarFileReader],
                                                     storageConf: Broadcast[SerializableConfiguration],
                                                     state: Broadcast[JavaSerializedValue[HoodieFileGroupReadState]])
  extends (PartitionedFile => Iterator[InternalRow]) with Serializable {

  import HoodieFileGroupReaderFunction._

  private def sparkAdapter = SparkAdapterSupport.sparkAdapter

  override def apply(file: PartitionedFile): Iterator[InternalRow] = {
    val s = state.value.value
    val conf = new HadoopStorageConfiguration(storageConf.value.value)
    val iter = file.partitionValues match {
      // Snapshot or incremental queries.
      case fileSliceMapping: HoodiePartitionFileSliceMapping =>
        val fileGroupName = FSUtils.getFileIdFromFilePath(sparkAdapter
          .getSparkPartitionedFileUtils.getPathFromPartitionedFile(file))
        fileSliceMapping.getSlice(fileGroupName) match {
          case Some(fileSlice) if !s.isCount && (s.requiredSchema.nonEmpty || fileSlice.getLogFiles.findAny().isPresent) =>
            val tableConfig = s.metaClient.getTableConfig
            // requiredFilters preserve Spark's row-level filtering semantics, while instantRangeOpt
            // keeps out-of-range records from participating in the file-group merge itself.
            val readerContext = new SparkFileFormatInternalRowReaderContext(
              fileGroupBaseFileReader.value, s.filters, s.requiredFilters, conf, tableConfig,
              sparkRequiredSchema = Some(s.requiredSchema), instantRangeOpt = s.instantRangeOpt)
            readerContext.enableLogicalTimestampFieldRepair(conf.getBoolean(ENABLE_LOGICAL_TIMESTAMP_REPAIR, true))
            val props = TypedProperties.copy(s.readerProps)
            val baseFileLength = if (fileSlice.getBaseFile.isPresent) {
              fileSlice.getBaseFile.get.getFileSize
            } else {
              0
            }
            val reader: HoodieRecordReader[InternalRow] =
              if (LsmReaderUtils.shouldUseLsmReader(
                tableConfig,
                ConfigUtils.getStringWithAltKeys(props, HoodieReaderConfig.MERGE_TYPE, true))) {
                HoodieLsmFileGroupReader.builder[InternalRow]()
                  .withReaderContext(readerContext)
                  .withHoodieTableMetaClient(s.metaClient)
                  .withLatestCommitTime(s.queryTimestamp)
                  .withBaseFileOption(fileSlice.getBaseFile)
                  .withLogFiles(fileSlice.getLogFiles)
                  .withPartitionPath(fileSlice.getPartitionPath)
                  .withDataSchema(s.dataSchema)
                  .withRequestedSchema(s.requestedSchema)
                  .withInternalSchemaOpt(s.internalSchemaOpt)
                  .withProps(props)
                  .withStart(file.start)
                  .withLength(baseFileLength)
                  .build()
              } else {
                HoodieFileGroupReader.builder[InternalRow]()
                  .withReaderContext(readerContext)
                  .withHoodieTableMetaClient(s.metaClient)
                  .withLatestCommitTime(s.queryTimestamp)
                  .withBaseFileOption(fileSlice.getBaseFile)
                  .withLogFiles(fileSlice.getLogFiles)
                  .withPartitionPath(fileSlice.getPartitionPath)
                  .withDataSchema(s.dataSchema)
                  .withRequestedSchema(s.requestedSchema)
                  .withInternalSchemaOpt(s.internalSchemaOpt)
                  .withProps(props)
                  .withStart(file.start)
                  .withLength(baseFileLength)
                  .withShouldUseRecordPosition(s.shouldUseRecordPosition)
                  .build()
              }
            // Append partition values to rows and project to output schema
            appendPartitionAndProject(
              reader.getClosableIterator,
              s.projectionInputSchema,
              s.remainingPartitionSchema,
              s.outputSchema,
              fileSliceMapping.getPartitionValues,
              s.fixedPartitionIndexes)

          case _ =>
            readBaseFile(file, s, conf)
        }
      // CDC queries.
      case cdcFileGroupMapping: HoodiePartitionCDCFileGroupMapping =>
        new CDCFileGroupIterator(
          HoodieCDCFileGroupSplit(cdcFileGroupMapping.getFileSplits().toArray),
          s.metaClient,
          conf,
          fileGroupBaseFileReader.value,
          s.tableSchema,
          HoodieCDCFileIndex.FULL_CDC_SPARK_SCHEMA,
          s.requiredSchema,
          TypedProperties.copy(s.cdcProps))

      case _ =>
        readBaseFile(file, s, conf)
    }
    CloseableIteratorListener.addListener(iter)
  }

  private def readBaseFile(file: PartitionedFile,
                           s: HoodieFileGroupReadState,
                           conf: StorageConfiguration[Configuration]): Iterator[InternalRow] = {
    val schemas = s.baseFileReadSchemas
    val hasVectors = schemas.readVectorColumns.nonEmpty
    val parquetFileReader = baseFileReader.value
    val filters = s.filters ++ s.requiredFilters
    val partitionSchema = s.partitionSchema
    val remainingPartitionSchema = s.remainingPartitionSchema

    val rawIter = if (remainingPartitionSchema.fields.length == partitionSchema.fields.length) {
      //none of partition fields are read from the file, so the reader will do the appending for us
      val iter = parquetFileReader.read(file, schemas.readRequiredSchema, partitionSchema, s.internalSchemaOpt, filters, conf,
        s.tableSchemaAsMessageType)
      projectIfNeeded(iter, StructType(schemas.readRequiredSchema.fields ++ partitionSchema.fields), schemas.outputSchema)
    } else if (remainingPartitionSchema.fields.length == 0) {
      //we read all of the partition fields from the file
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      //we need to modify the partitioned file so that the partition values are empty
      val modifiedFile = pfileUtils.createPartitionedFile(InternalRow.empty, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      val readSchema = StructType(schemas.readRequiredSchema.fields ++ partitionSchema.fields)
      //and we pass an empty schema for the partition schema
      val iter = parquetFileReader.read(modifiedFile, readSchema, new StructType(), s.internalSchemaOpt, filters, conf,
        s.tableSchemaAsMessageType)
      projectIfNeeded(iter, readSchema, schemas.outputSchema)
    } else {
      //need to do an additional projection here. The case in mind is that partition schema is "a,b,c" mandatoryFields is "a,c",
      //then we will read (dataSchema + a + c) and append b. So the final schema will be (data schema + a + c +b)
      //but expected output is (data schema + a + b + c)
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      val partitionValues = getFixedPartitionValues(file.partitionValues, partitionSchema, s.fixedPartitionIndexes)
      val modifiedFile = pfileUtils.createPartitionedFile(partitionValues, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      val iter = parquetFileReader.read(modifiedFile, schemas.requestedSchema, remainingPartitionSchema, s.internalSchemaOpt, filters, conf,
        s.tableSchemaAsMessageType)
      projectIter(iter, StructType(schemas.requestedSchema.fields ++ remainingPartitionSchema.fields), schemas.outputSchema)
    }

    if (hasVectors) {
      // The raw iterator has BinaryType for vector columns; convert back to ArrayType.
      // All branches above produce rows in the rewritten output schema: filter-only columns from
      // readRequiredSchema are projected away by projectIfNeeded/projectIter.
      wrapWithVectorConversion(rawIter, schemas.outputSchema, s.outputSchema, schemas.outputVectorColumns)
    } else {
      rawIter
    }
  }
}

private[parquet] object HoodieFileGroupReaderFunction {

  private def appendPartitionAndProject(iter: ClosableIterator[InternalRow],
                                        inputSchema: StructType,
                                        partitionSchema: StructType,
                                        to: StructType,
                                        partitionValues: InternalRow,
                                        fixedPartitionIndexes: Set[Int]): Iterator[InternalRow] = {
    if (partitionSchema.isEmpty) {
      //'inputSchema' and 'to' should be the same so the projection will just be an identity func
      projectSchema(iter, inputSchema, to)
    } else {
      val fixedPartitionValues = if (partitionSchema.length == partitionValues.numFields) {
        //need to append all of the partition fields
        partitionValues
      } else {
        //some partition fields read from file, some were not
        getFixedPartitionValues(partitionValues, partitionSchema, fixedPartitionIndexes)
      }
      val unsafeProjection = generateOutputProjection(StructType(inputSchema.fields ++ partitionSchema.fields), to)
      val joinedRow = new JoinedRow()
      makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(joinedRow(d, fixedPartitionValues)))
    }
  }

  private def projectSchema(iter: ClosableIterator[InternalRow],
                            from: StructType,
                            to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateOutputProjection(from, to)
    makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(d))
  }

  /**
   * The scan's output projection. Stays the by-name top-level projection unless a column comes out of the
   * reader with nested fields Spark did not ask for (see `projectionInputSchema` in
   * buildReaderWithPartitionValues), in which case those are dropped by name at every depth.
   */
  private def generateOutputProjection(from: StructType, to: StructType): UnsafeProjection = {
    val hasWiderNestedInput = to.fields.exists { f =>
      from.getFieldIndex(f.name).exists(i => SparkSchemaTransformUtils.needsNestedPruning(from.fields(i).dataType, f.dataType))
    }
    if (hasWiderNestedInput) {
      SparkSchemaTransformUtils.generateNestedPruningProjection(from, to)
    } else {
      generateUnsafeProjection(from, to)
    }
  }

  private def makeCloseableFileGroupMappingRecordIterator(closeableFileGroupRecordIterator: ClosableIterator[InternalRow],
                                                          mappingFunction: Function[InternalRow, InternalRow]): Iterator[InternalRow] = {
    CloseableIteratorListener.addListener(closeableFileGroupRecordIterator)
    new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closeableFileGroupRecordIterator.hasNext

      override def next(): InternalRow = mappingFunction(closeableFileGroupRecordIterator.next())

      override def close(): Unit = closeableFileGroupRecordIterator.close()
    }
  }

  /**
   * Wraps an iterator to convert binary VECTOR columns back to typed arrays.
   * The read schema has BinaryType for vector columns; the target schema has ArrayType.
   */
  private def wrapWithVectorConversion(iter: Iterator[InternalRow],
                                       readSchema: StructType,
                                       targetSchema: StructType,
                                       vectorCols: Map[Int, HoodieSchema.Vector]): Iterator[InternalRow] = {
    val vectorProjection = UnsafeProjection.create(targetSchema)
    val javaVectorCols: java.util.Map[Integer, HoodieSchema.Vector] =
      vectorCols.map { case (k, v) => (Integer.valueOf(k), v) }.asJava
    val mapper = VectorConversionUtils.buildRowMapper(readSchema, javaVectorCols, vectorProjection.apply(_))
    iter.map(mapper.apply(_))
  }

  private def projectIter(iter: Iterator[Any], from: StructType, to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    val batchProjection = ColumnarBatchUtils.generateProjection(from, to)
    iter.map {
      case ir: InternalRow => unsafeProjection(ir)
      case cb: ColumnarBatch => batchProjection(cb)
    }.asInstanceOf[Iterator[InternalRow]]
  }

  /**
   * Projects to `to` only when the read schema was augmented with filter-only columns;
   * otherwise returns the iterator as is, preserving columnar batches.
   */
  private def projectIfNeeded(iter: Iterator[InternalRow], from: StructType, to: StructType): Iterator[InternalRow] = {
    if (from.fieldNames.sameElements(to.fieldNames)) {
      iter
    } else {
      projectIter(iter, from, to)
    }
  }

  private def getFixedPartitionValues(allPartitionValues: InternalRow, partitionSchema: StructType, fixedPartitionIndexes: Set[Int]): InternalRow = {
    InternalRow.fromSeq(allPartitionValues.toSeq(partitionSchema).zipWithIndex.filter(p => fixedPartitionIndexes.contains(p._2)).map(p => p._1))
  }
}
