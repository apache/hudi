/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.HoodieBaseRelation.BaseFileReader
import org.apache.hudi.HoodieConversionUtils.{toJavaOption, toScalaOption}
import org.apache.hudi.HoodieDataSourceHelper.AvroDeserializerSupport
import org.apache.hudi.LogFileIterator.{getPartitionPath, scanLog}
import org.apache.hudi.avro.AvroSchemaCache
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMemoryConfig, HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.engine.{EngineType, HoodieLocalEngineContext}
import org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath
import org.apache.hudi.common.model.{HoodieAvroIndexedRecord, HoodieEmptyRecord, HoodieLogFile, HoodieOperation, HoodieRecord, HoodieSparkRecord}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner
import org.apache.hudi.common.util.{FileIOUtils, HoodieRecordUtils}
import org.apache.hudi.config.HoodiePayloadConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata}
import org.apache.hudi.metadata.HoodieTableMetadata.getDataTableBasePathFromMetadataTable
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}
import org.apache.hudi.util.CachingIterator

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.types.StructType

import java.io.Closeable

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Provided w/ list of log files, iterates over all of the records stored in
 * Delta Log files (represented as [[InternalRow]]s)
 */
class LogFileIterator(logFiles: List[HoodieLogFile],
                      partitionPath: StoragePath,
                      tableSchema: HoodieTableSchema,
                      requiredStructTypeSchema: StructType,
                      requiredAvroSchema: Schema,
                      tableState: HoodieTableState,
                      config: Configuration)
  extends CachingIterator[InternalRow] with AvroDeserializerSupport {

  def this(logFiles: List[HoodieLogFile],
           partitionPath: StoragePath,
           tableSchema: HoodieTableSchema,
           requiredSchema: HoodieTableSchema,
           tableState: HoodieTableState,
           config: Configuration) {
    this(logFiles, partitionPath, tableSchema, requiredSchema.structTypeSchema,
      new Schema.Parser().parse(requiredSchema.avroSchemaStr), tableState, config)
  }
  def this(split: HoodieMergeOnReadFileSplit,
           tableSchema: HoodieTableSchema,
           requiredSchema: HoodieTableSchema,
           tableState: HoodieTableState,
           config: Configuration) {
    this(split.logFiles, getPartitionPath(split), tableSchema, requiredSchema, tableState, config)
  }
  private val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  protected val payloadProps: TypedProperties = tableState.preCombineFieldOpt
    .map { preCombineField =>
      HoodiePayloadConfig.newBuilder
        .withPayloadOrderingField(preCombineField)
        .build
        .getProps
    }
    .getOrElse(new TypedProperties())

  protected override val avroSchema: Schema = requiredAvroSchema
  protected override val structTypeSchema: StructType = requiredStructTypeSchema

  protected val logFileReaderAvroSchema: Schema = AvroSchemaCache.intern(new Schema.Parser().parse(tableSchema.avroSchemaStr))
  protected val logFileReaderStructType: StructType = tableSchema.structTypeSchema

  private val requiredSchemaAvroProjection: AvroProjection = AvroProjection.create(avroSchema)
  private val requiredSchemaRowProjection: Projection = generateUnsafeProjection(logFileReaderStructType, structTypeSchema)

  private val logRecords = {
    val internalSchema = tableSchema.internalSchema.getOrElse(InternalSchema.getEmptyInternalSchema)

    scanLog(logFiles, partitionPath, logFileReaderAvroSchema, tableState,
      maxCompactionMemoryInBytes, config, internalSchema)
  }

  private val (hasOperationField, operationFieldPos) = {
    val operationField = logFileReaderAvroSchema.getField(HoodieRecord.OPERATION_METADATA_FIELD)
    if (operationField != null) {
      (true, operationField.pos())
    } else {
      (false, -1)
    }
  }

  protected def isDeleteOperation(r: InternalRow): Boolean = if (hasOperationField) {
    val operation = r.getString(operationFieldPos)
    HoodieOperation.fromName(operation) == HoodieOperation.DELETE
  } else {
    false
  }

  protected def isDeleteOperation(r: GenericRecord): Boolean = if (hasOperationField) {
    val operation = r.get(operationFieldPos).toString
    HoodieOperation.fromName(operation) == HoodieOperation.DELETE
  } else {
    false
  }

  def logRecordsPairIterator(): Iterator[(String, HoodieRecord[_])] = {
    logRecords.iterator
  }

  // NOTE: This have to stay lazy to make sure it's initialized only at the point where it's
  //       going to be used, since we modify `logRecords` before that and therefore can't do it any earlier
  private lazy val logRecordsIterator: Iterator[Option[HoodieRecord[_]]] =
    logRecords.iterator.map {
      case (_, record: HoodieSparkRecord) => Option(record)
      case (_, _: HoodieEmptyRecord[_]) => Option.empty
      case (_, record) =>
        toScalaOption(record.toIndexedRecord(logFileReaderAvroSchema, payloadProps))

    }

  protected def removeLogRecord(key: String): Option[HoodieRecord[_]] = logRecords.remove(key)

  protected def doHasNext: Boolean = hasNextInternal

  // NOTE: It's crucial for this method to be annotated w/ [[@tailrec]] to make sure
  //       that recursion is unfolded into a loop to avoid stack overflows while
  //       handling records
  @tailrec private def hasNextInternal: Boolean = {
    logRecordsIterator.hasNext && {
      logRecordsIterator.next() match {
        case Some(r: HoodieAvroIndexedRecord) =>
          val data = r.getData.asInstanceOf[GenericRecord]
          if (isDeleteOperation(data)) {
            this.hasNextInternal
          } else {
            val projectedAvroRecord = requiredSchemaAvroProjection(data)
            nextRecord = deserialize(projectedAvroRecord)
            true
          }
        case Some(r: HoodieSparkRecord) =>
          val data = r.getData
          if (isDeleteOperation(data)) {
            this.hasNextInternal
          } else {
            nextRecord = requiredSchemaRowProjection(data)
            true
          }
        case None => this.hasNextInternal
      }
    }
  }
}

/**
 * Provided w/ list of log files and base file iterator, provides an iterator over all of the records stored in
 * Base file as well as all of the Delta Log files simply returning concatenation of these streams, while not
 * performing any combination/merging of the records w/ the same primary keys (ie producing duplicates potentially)
 */
class SkipMergeIterator(logFiles: List[HoodieLogFile],
                        partitionPath: StoragePath,
                        baseFileIterator: Iterator[InternalRow],
                        readerSchema: StructType,
                        dataSchema: HoodieTableSchema,
                        requiredStructTypeSchema: StructType,
                        requiredAvroSchema: Schema,
                        tableState: HoodieTableState,
                        config: Configuration)
  extends LogFileIterator(logFiles, partitionPath, dataSchema, requiredStructTypeSchema, requiredAvroSchema, tableState, config) {

  def this(split: HoodieMergeOnReadFileSplit, baseFileReader: BaseFileReader, dataSchema: HoodieTableSchema,
           requiredSchema: HoodieTableSchema, tableState: HoodieTableState, config: Configuration) {
    this(split.logFiles, getPartitionPath(split), baseFileReader(split.dataFile.get),
      baseFileReader.schema, dataSchema, requiredSchema.structTypeSchema,
      new Schema.Parser().parse(requiredSchema.avroSchemaStr), tableState, config)
  }

  private val requiredSchemaProjection = generateUnsafeProjection(readerSchema, structTypeSchema)

  override def doHasNext: Boolean = {
    if (baseFileIterator.hasNext) {
      // No merge is required, simply load current row and project into required schema
      nextRecord = requiredSchemaProjection(baseFileIterator.next())
      true
    } else {
      super[LogFileIterator].doHasNext
    }
  }
}

/**
 * Provided w/ list of log files and base file iterator, provides an iterator over all of the records stored in
 * a) Base file and all of the b) Delta Log files combining records with the same primary key from both of these
 * streams
 */
class RecordMergingFileIterator(logFiles: List[HoodieLogFile],
                                partitionPath: StoragePath,
                                baseFileIterator: Iterator[InternalRow],
                                readerSchema: StructType,
                                dataSchema: HoodieTableSchema,
                                requiredStructTypeSchema: StructType,
                                requiredAvroSchema: Schema,
                                tableState: HoodieTableState,
                                config: Configuration)
  extends LogFileIterator(logFiles, partitionPath, dataSchema, requiredStructTypeSchema, requiredAvroSchema, tableState, config) {

  def this(logFiles: List[HoodieLogFile],
           partitionPath: StoragePath,
           baseFileIterator: Iterator[InternalRow],
           readerSchema: StructType,
           dataSchema: HoodieTableSchema,
           requiredSchema: HoodieTableSchema,
           tableState: HoodieTableState,
           config: Configuration) {
    this(logFiles, partitionPath, baseFileIterator, readerSchema, dataSchema, requiredSchema.structTypeSchema,
      new Schema.Parser().parse(requiredSchema.avroSchemaStr), tableState, config)
  }
  def this(split: HoodieMergeOnReadFileSplit, baseFileReader: BaseFileReader, dataSchema: HoodieTableSchema,
           requiredSchema: HoodieTableSchema, tableState: HoodieTableState, config: Configuration) {
    this(split.logFiles, getPartitionPath(split), baseFileReader(split.dataFile.get),
      baseFileReader.schema, dataSchema, requiredSchema, tableState, config)
  }

  // NOTE: Record-merging iterator supports 2 modes of operation merging records bearing either
  //        - Full table's schema
  //        - Projected schema
  //       As such, no particular schema could be assumed, and therefore we rely on the caller
  //       to correspondingly set the schema of the expected output of base-file reader
  private val baseFileReaderAvroSchema = AvroSchemaCache.intern(sparkAdapter.getAvroSchemaConverters.toAvroType(readerSchema, nullable = false, "record"))

  private val serializer = sparkAdapter.createAvroSerializer(readerSchema, baseFileReaderAvroSchema, nullable = false)

  private val recordKeyOrdinal = readerSchema.fieldIndex(tableState.recordKeyField)

  private val requiredSchemaProjection = generateUnsafeProjection(readerSchema, structTypeSchema)
  private val requiredSchemaAvroProjection = AvroProjection.create(avroSchema)

  private val recordMerger = HoodieRecordUtils.createRecordMerger(tableState.tablePath, EngineType.SPARK,
    tableState.recordMergeImplClasses.asJava, tableState.recordMergeStrategyId)

  override def doHasNext: Boolean = hasNextInternal

  // NOTE: It's crucial for this method to be annotated w/ [[@tailrec]] to make sure
  //       that recursion is unfolded into a loop to avoid stack overflows while
  //       handling records
  @tailrec private def hasNextInternal: Boolean = {
    if (baseFileIterator.hasNext) {
      val curRow = baseFileIterator.next()
      val curKey = curRow.getString(recordKeyOrdinal)
      val updatedRecordOpt = removeLogRecord(curKey)
      if (updatedRecordOpt.isEmpty) {
        // No merge is required, simply load current row and project into required schema
        nextRecord = requiredSchemaProjection(curRow)
        true
      } else {
       val mergedRecordOpt = merge(curRow, updatedRecordOpt.get)
        if (mergedRecordOpt.isEmpty) {
          // Record has been deleted, skipping
          this.hasNextInternal
        } else {
          nextRecord = mergedRecordOpt.get
          true
        }
      }
    } else {
      super[LogFileIterator].doHasNext
    }
  }

  private def serialize(curRowRecord: InternalRow): GenericRecord =
    serializer.serialize(curRowRecord).asInstanceOf[GenericRecord]

  private def merge(curRow: InternalRow, newRecord: HoodieRecord[_]): Option[InternalRow] = {
    // NOTE: We have to pass in Avro Schema used to read from Delta Log file since we invoke combining API
    //       on the record from the Delta Log
    recordMerger.getRecordType match {
      case HoodieRecordType.SPARK =>
        val curRecord = new HoodieSparkRecord(curRow, readerSchema)
        val result = recordMerger.merge(curRecord, baseFileReaderAvroSchema, newRecord, logFileReaderAvroSchema, payloadProps)
        toScalaOption(result)
          .flatMap { r =>
            val data = r.getLeft.getData.asInstanceOf[InternalRow]
            if (isDeleteOperation(data)) {
              None
            } else {
              val schema = HoodieInternalRowUtils.getCachedSchema(r.getRight)
              val projection = HoodieInternalRowUtils.getCachedUnsafeProjection(schema, structTypeSchema)
              Some(projection.apply(data))
            }
          }
      case _ =>
        val curRecord = new HoodieAvroIndexedRecord(serialize(curRow))
        val result = recordMerger.merge(curRecord, baseFileReaderAvroSchema, newRecord, logFileReaderAvroSchema, payloadProps)
        toScalaOption(result)
          .flatMap { r =>
            val avroRecord = r.getLeft.toIndexedRecord(r.getRight, payloadProps).get.getData.asInstanceOf[GenericRecord]
            if (isDeleteOperation(avroRecord)) {
              None
            } else {
              Some(deserialize(requiredSchemaAvroProjection(avroRecord)))
            }
          }
    }
  }
}

object LogFileIterator extends SparkAdapterSupport {

  def scanLog(logFiles: List[HoodieLogFile],
              partitionPath: StoragePath,
              logSchema: Schema,
              tableState: HoodieTableState,
              maxCompactionMemoryInBytes: Long,
              hadoopConf: Configuration,
              internalSchema: InternalSchema = InternalSchema.getEmptyInternalSchema): mutable.Map[String, HoodieRecord[_]] = {
    val tablePath = tableState.tablePath
    val storage = HoodieStorageUtils.getStorage(tablePath, HadoopFSUtils.getStorageConf(hadoopConf))

    val logRecordScannerBuilder = HoodieMergedLogRecordScanner.newBuilder()
      .withStorage(storage)
      .withBasePath(tablePath)
      .withLogFilePaths(logFiles.map(logFile => logFile.getPath.toString).asJava)
      .withReaderSchema(logSchema)
      // NOTE: This part shall only be reached when at least one log is present in the file-group
      //       entailing that table has to have at least one commit
      .withLatestInstantTime(tableState.latestCommitTimestamp.get)
      .withReverseReader(false)
      .withInternalSchema(internalSchema)
      .withBufferSize(
        hadoopConf.getInt(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.key(),
          HoodieMemoryConfig.DEFAULT_MR_MAX_DFS_STREAM_BUFFER_SIZE))
      .withMaxMemorySizeInBytes(maxCompactionMemoryInBytes)
      .withSpillableMapBasePath(
        hadoopConf.get(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key,
          FileIOUtils.getDefaultSpillableMapBasePath))
      .withDiskMapType(
        hadoopConf.getEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key,
          HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue))
      .withBitCaskDiskMapCompressionEnabled(
        hadoopConf.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
          HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))

    if (logFiles.nonEmpty) {
      logRecordScannerBuilder.withPartition(getRelativePartitionPath(
        new StoragePath(tableState.tablePath), logFiles.head.getPath.getParent))
    }

    logRecordScannerBuilder.withRecordMerger(
      HoodieRecordUtils.createRecordMerger(tableState.tablePath, EngineType.SPARK, tableState.recordMergeImplClasses.asJava, tableState.recordMergeStrategyId))

    val scanner = logRecordScannerBuilder.build()

    closing(scanner) {
      // NOTE: We have to copy record-map (by default immutable copy is exposed)
      mutable.HashMap(scanner.getRecords.asScala.toSeq: _*)
    }
  }

  def closing[T](c: Closeable)(f: => T): T = {
    try { f } finally {
      c.close()
    }
  }

  def getPartitionPath(split: HoodieMergeOnReadFileSplit): StoragePath = {
    // Determine partition path as an immediate parent folder of either
    //    - The base file
    //    - Some log file
    split.dataFile.map(baseFile =>
      sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(baseFile))
      .getOrElse(split.logFiles.head.getPath)
      .getParent
  }
}
