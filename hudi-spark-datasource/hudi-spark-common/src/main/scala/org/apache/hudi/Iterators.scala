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

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.HoodieBaseRelation.BaseFileReader
import org.apache.hudi.HoodieConversionUtils.{toJavaOption, toScalaOption}
import org.apache.hudi.HoodieDataSourceHelper.AvroDeserializerSupport
import org.apache.hudi.LogFileIterator._
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.engine.{EngineType, HoodieLocalEngineContext}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner
import org.apache.hudi.common.util.HoodieRecordUtils
import org.apache.hudi.config.HoodiePayloadConfig
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.metadata.HoodieTableMetadata.getDataTableBasePathFromMetadataTable
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata}
import org.apache.hudi.util.CachingIterator
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.HoodieInternalRowUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Projection
import org.apache.spark.sql.types.StructType

import java.io.Closeable
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

/**
 * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], iterates over all of the records stored in
 * Delta Log files (represented as [[InternalRow]]s)
 */
class LogFileIterator(split: HoodieMergeOnReadFileSplit,
                      tableSchema: HoodieTableSchema,
                      requiredSchema: HoodieTableSchema,
                      tableState: HoodieTableState,
                      config: Configuration)
  extends CachingIterator[InternalRow] with AvroDeserializerSupport {

  private val maxCompactionMemoryInBytes: Long = getMaxCompactionMemoryInBytes(new JobConf(config))

  protected val payloadProps: TypedProperties = tableState.preCombineFieldOpt
    .map { preCombineField =>
      HoodiePayloadConfig.newBuilder
        .withPayloadOrderingField(preCombineField)
        .build
        .getProps
    }
    .getOrElse(new TypedProperties())

  protected override val avroSchema: Schema = new Schema.Parser().parse(requiredSchema.avroSchemaStr)
  protected override val structTypeSchema: StructType = requiredSchema.structTypeSchema

  protected val logFileReaderAvroSchema: Schema = new Schema.Parser().parse(tableSchema.avroSchemaStr)
  protected val logFileReaderStructType: StructType = tableSchema.structTypeSchema

  private val requiredSchemaAvroProjection: AvroProjection = AvroProjection.create(avroSchema)
  private val requiredSchemaRowProjection: Projection = generateUnsafeProjection(logFileReaderStructType, structTypeSchema)

  private val logRecords = {
    val internalSchema = tableSchema.internalSchema.getOrElse(InternalSchema.getEmptyInternalSchema)

    scanLog(split.logFiles, getPartitionPath(split), logFileReaderAvroSchema, tableState,
      maxCompactionMemoryInBytes, config, internalSchema)
  }

  def getPartitionPath(split: HoodieMergeOnReadFileSplit): Path = {
    // Determine partition path as an immediate parent folder of either
    //    - The base file
    //    - Some log file
    split.dataFile.map(baseFile =>
      sparkAdapter.getSparkPartitionedFileUtils.getPathFromPartitionedFile(baseFile))
      .getOrElse(split.logFiles.head.getPath)
      .getParent
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
          val projectedAvroRecord = requiredSchemaAvroProjection(r.getData.asInstanceOf[GenericRecord])
          nextRecord = deserialize(projectedAvroRecord)
          true
        case Some(r: HoodieSparkRecord) =>
          nextRecord = requiredSchemaRowProjection(r.getData)
          true
        case None => this.hasNextInternal
      }
    }
  }
}

/**
 * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], provides an iterator over all of the records stored in
 * Base file as well as all of the Delta Log files simply returning concatenation of these streams, while not
 * performing any combination/merging of the records w/ the same primary keys (ie producing duplicates potentially)
 */
private class SkipMergeIterator(split: HoodieMergeOnReadFileSplit,
                                baseFileReader: BaseFileReader,
                                dataSchema: HoodieTableSchema,
                                requiredSchema: HoodieTableSchema,
                                tableState: HoodieTableState,
                                config: Configuration)
  extends LogFileIterator(split, dataSchema, requiredSchema, tableState, config) {

  private val requiredSchemaProjection = generateUnsafeProjection(baseFileReader.schema, structTypeSchema)

  private val baseFileIterator = baseFileReader(split.dataFile.get)

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
 * Provided w/ instance of [[HoodieMergeOnReadFileSplit]], provides an iterator over all of the records stored in
 * a) Base file and all of the b) Delta Log files combining records with the same primary key from both of these
 * streams
 */
class RecordMergingFileIterator(split: HoodieMergeOnReadFileSplit,
                                baseFileReader: BaseFileReader,
                                dataSchema: HoodieTableSchema,
                                requiredSchema: HoodieTableSchema,
                                tableState: HoodieTableState,
                                config: Configuration)
  extends LogFileIterator(split, dataSchema, requiredSchema, tableState, config) {

  // NOTE: Record-merging iterator supports 2 modes of operation merging records bearing either
  //        - Full table's schema
  //        - Projected schema
  //       As such, no particular schema could be assumed, and therefore we rely on the caller
  //       to correspondingly set the schema of the expected output of base-file reader
  private val baseFileReaderAvroSchema = sparkAdapter.getAvroSchemaConverters.toAvroType(baseFileReader.schema, nullable = false, "record")

  private val serializer = sparkAdapter.createAvroSerializer(baseFileReader.schema, baseFileReaderAvroSchema, nullable = false)

  private val recordKeyOrdinal = baseFileReader.schema.fieldIndex(tableState.recordKeyField)

  private val requiredSchemaProjection = generateUnsafeProjection(baseFileReader.schema, structTypeSchema)
  private val requiredSchemaAvroProjection = AvroProjection.create(avroSchema)

  private val baseFileIterator = baseFileReader(split.dataFile.get)

  private val recordMerger = HoodieRecordUtils.createRecordMerger(tableState.tablePath, EngineType.SPARK,
    tableState.recordMergerImpls.asJava, tableState.recordMergerStrategy)

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
        val curRecord = new HoodieSparkRecord(curRow, baseFileReader.schema)
        val result = recordMerger.merge(curRecord, baseFileReaderAvroSchema, newRecord, logFileReaderAvroSchema, payloadProps)
        toScalaOption(result)
          .map { r =>
            val schema = HoodieInternalRowUtils.getCachedSchema(r.getRight)
            val projection = HoodieInternalRowUtils.getCachedUnsafeProjection(schema, structTypeSchema)
            projection.apply(r.getLeft.getData.asInstanceOf[InternalRow])
          }
      case _ =>
        val curRecord = new HoodieAvroIndexedRecord(serialize(curRow))
        val result = recordMerger.merge(curRecord, baseFileReaderAvroSchema, newRecord, logFileReaderAvroSchema, payloadProps)
        toScalaOption(result)
          .map { r =>
            val avroRecord = r.getLeft.toIndexedRecord(r.getRight, payloadProps).get.getData.asInstanceOf[GenericRecord]
            deserialize(requiredSchemaAvroProjection(avroRecord))
          }
    }
  }
}

object LogFileIterator {

  def scanLog(logFiles: List[HoodieLogFile],
              partitionPath: Path,
              logSchema: Schema,
              tableState: HoodieTableState,
              maxCompactionMemoryInBytes: Long,
              hadoopConf: Configuration,
              internalSchema: InternalSchema = InternalSchema.getEmptyInternalSchema): mutable.Map[String, HoodieRecord[_]] = {
    val tablePath = tableState.tablePath
    val fs = FSUtils.getFs(tablePath, hadoopConf)

    if (HoodieTableMetadata.isMetadataTable(tablePath)) {
      val metadataConfig = HoodieMetadataConfig.newBuilder()
        .fromProperties(tableState.metadataConfig.getProps)
        .withSpillableMapDir(hadoopConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP, HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))
        .enable(true).build()
      val dataTableBasePath = getDataTableBasePathFromMetadataTable(tablePath)
      val metadataTable = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(hadoopConf), metadataConfig,
        dataTableBasePath)

      // We have to force full-scan for the MT log record reader, to make sure
      // we can iterate over all of the partitions, since by default some of the partitions (Column Stats,
      // Bloom Filter) are in "point-lookup" mode
      val forceFullScan = true

      // NOTE: In case of Metadata Table partition path equates to partition name (since there's just one level
      //       of indirection among MT partitions)
      val relativePartitionPath = getRelativePartitionPath(new Path(tablePath), partitionPath)

      val logRecordReader =
        metadataTable.getLogRecordScanner(logFiles.asJava, relativePartitionPath, toJavaOption(Some(forceFullScan)))
          .getLeft

      val recordList = closing(logRecordReader) {
        logRecordReader.getRecords
      }

      mutable.HashMap(recordList.asScala.map(r => (r.getRecordKey, r)): _*)
    } else {
      val logRecordScannerBuilder = HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(tablePath)
        .withLogFilePaths(logFiles.map(logFile => logFile.getPath.toString).asJava)
        .withReaderSchema(logSchema)
        // NOTE: This part shall only be reached when at least one log is present in the file-group
        //       entailing that table has to have at least one commit
        .withLatestInstantTime(tableState.latestCommitTimestamp.get)
        .withReadBlocksLazily(
          Try(hadoopConf.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP,
            HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED).toBoolean)
            .getOrElse(false))
        .withReverseReader(false)
        .withInternalSchema(internalSchema)
        .withBufferSize(
          hadoopConf.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP,
            HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withMaxMemorySizeInBytes(maxCompactionMemoryInBytes)
        .withSpillableMapBasePath(
          hadoopConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP,
            HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))
        .withDiskMapType(
          hadoopConf.getEnum(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key,
            HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue))
        .withBitCaskDiskMapCompressionEnabled(
          hadoopConf.getBoolean(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
            HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue()))

      if (logFiles.nonEmpty) {
        logRecordScannerBuilder.withPartition(
          getRelativePartitionPath(new Path(tableState.tablePath), logFiles.head.getPath.getParent))
      }

      logRecordScannerBuilder.withRecordMerger(
        HoodieRecordUtils.createRecordMerger(tableState.tablePath, EngineType.SPARK, tableState.recordMergerImpls.asJava, tableState.recordMergerStrategy))

      val scanner = logRecordScannerBuilder.build()

      closing(scanner) {
        // NOTE: We have to copy record-map (by default immutable copy is exposed)
        mutable.HashMap(scanner.getRecords.asScala.toSeq: _*)
      }
    }
  }

  def closing[T](c: Closeable)(f: => T): T = {
    try { f } finally {
      c.close()
    }
  }
}
