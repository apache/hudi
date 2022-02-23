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
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder, IndexedRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.HoodieCommonUtils.toScalaOption
import org.apache.hudi.HoodieDataSourceHelper._
import org.apache.hudi.HoodieMergeOnReadRDD.resolveAvroSchemaNullability
import org.apache.hudi.MergeOnReadSnapshotRelation.getFilePath
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, HoodieRecordPayload}
import org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner
import org.apache.hudi.config.HoodiePayloadConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig
import org.apache.hudi.metadata.HoodieTableMetadata.getDataTableBasePathFromMetadataTable
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadata}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}

import java.io.Closeable
import java.util.Properties
import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.util.Try

case class HoodieMergeOnReadPartition(index: Int, split: HoodieMergeOnReadFileSplit) extends Partition

class HoodieMergeOnReadRDD(@transient sc: SparkContext,
                           @transient config: Configuration,
                           fullSchemaFileReader: PartitionedFile => Iterator[InternalRow],
                           requiredSchemaFileReader: PartitionedFile => Iterator[InternalRow],
                           tableState: HoodieTableState,
                           tableSchema: HoodieTableSchema,
                           requiredSchema: HoodieTableSchema,
                           @transient fileSplits: Seq[HoodieMergeOnReadFileSplit])
  extends RDD[InternalRow](sc, Nil) with HoodieUnsafeRDD {

  private val confBroadcast = sc.broadcast(new SerializableWritable(config))
  private val recordKeyField = tableState.recordKeyField
  private val payloadProps = tableState.preCombineFieldOpt
    .map(preCombineField =>
      HoodiePayloadConfig.newBuilder
        .withPayloadOrderingField(preCombineField)
        .build
        .getProps
    )
    .getOrElse(new Properties())

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val mergeOnReadPartition = split.asInstanceOf[HoodieMergeOnReadPartition]
    val iter = mergeOnReadPartition.split match {
      case dataFileOnlySplit if dataFileOnlySplit.logFiles.isEmpty =>
        requiredSchemaFileReader(dataFileOnlySplit.dataFile.get)

      case logFileOnlySplit if logFileOnlySplit.dataFile.isEmpty =>
        logFileIterator(logFileOnlySplit, getConfig)

      case skipMergeSplit if skipMergeSplit.mergeType.equals(DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL) =>
        skipMergeFileIterator(skipMergeSplit, requiredSchemaFileReader(skipMergeSplit.dataFile.get), getConfig)

      case payloadCombineSplit
        if payloadCombineSplit.mergeType.equals(DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL) =>
        payloadCombineFileIterator(payloadCombineSplit, fullSchemaFileReader(payloadCombineSplit.dataFile.get),
          getConfig)

      case _ => throw new HoodieException(s"Unable to select an Iterator to read the Hoodie MOR File Split for " +
        s"file path: ${mergeOnReadPartition.split.dataFile.get.filePath}" +
        s"log paths: ${mergeOnReadPartition.split.logFiles.toString}" +
        s"hoodie table path: ${mergeOnReadPartition.split.tablePath}" +
        s"spark partition Index: ${mergeOnReadPartition.index}" +
        s"merge type: ${mergeOnReadPartition.split.mergeType}")
    }
    if (iter.isInstanceOf[Closeable]) {
      // register a callback to close logScanner which will be executed on task completion.
      // when tasks finished, this method will be called, and release resources.
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => iter.asInstanceOf[Closeable].close()))
    }
    iter
  }

  override protected def getPartitions: Array[Partition] =
    fileSplits.zipWithIndex.map(file => HoodieMergeOnReadPartition(file._2, file._1)).toArray

  private def getConfig: Configuration = {
    val conf = confBroadcast.value.value
    HoodieMergeOnReadRDD.CONFIG_INSTANTIATION_LOCK.synchronized {
      new Configuration(conf)
    }
  }

  private def logFileIterator(split: HoodieMergeOnReadFileSplit,
                              config: Configuration): Iterator[InternalRow] =
    new LogFileIterator(split, config)

  private def skipMergeFileIterator(split: HoodieMergeOnReadFileSplit,
                                    baseFileIterator: Iterator[InternalRow],
                                    config: Configuration): Iterator[InternalRow] =
    new SkipMergeIterator(split, baseFileIterator, config)

  private def payloadCombineFileIterator(split: HoodieMergeOnReadFileSplit,
                                         baseFileIterator: Iterator[InternalRow],
                                         config: Configuration): Iterator[InternalRow] =
    new RecordMergingFileIterator(split, baseFileIterator, config)

  class RecordMergingFileIterator(split: HoodieMergeOnReadFileSplit,
                                  baseFileIterator: Iterator[InternalRow],
                                  config: Configuration)
    extends LogFileIterator(split, config) {

    private val serializer = sparkAdapter.createAvroSerializer(tableSchema.structTypeSchema, tableAvroSchema,
      resolveAvroSchemaNullability(tableAvroSchema))
    private val recordKeyPosition = tableSchema.structTypeSchema.fieldIndex(recordKeyField)

    override def hasNext: Boolean = {
      if (baseFileIterator.hasNext) {
        val curRowRecord = baseFileIterator.next()
        val curKey = curRowRecord.getString(recordKeyPosition)
        val updatedRecordOpt = logRecords.remove(curKey)
        if (updatedRecordOpt.nonEmpty) {
          val mergedAvroRecord = merge(curRowRecord, updatedRecordOpt.get)
          if (mergedAvroRecord.isEmpty) {
            // Record has been deleted, skipping
            this.hasNext
          } else {
            // Load merged record as InternalRow with required schema
            recordToLoad = deserialize(mergedAvroRecord.get.asInstanceOf[GenericRecord])
            true
          }
        } else {
          // No merge needed, load current row with required schema
          recordToLoad = unsafeProjection(createInternalRowWithSchema(curRowRecord, requiredSchema.structTypeSchema, requiredFieldPosition))
          true
        }
      } else {
        super[LogFileIterator].hasNext
      }
    }

    private def merge(curRow: InternalRow, newRecord: HoodieRecord[_ <: HoodieRecordPayload[_]]): Option[IndexedRecord] = {
      val curAvroRecord = serializer.serialize(curRow).asInstanceOf[GenericRecord]
      toScalaOption(newRecord.getData.combineAndGetUpdateValue(curAvroRecord, tableAvroSchema, payloadProps))
    }
  }

  class LogFileIterator(split: HoodieMergeOnReadFileSplit, config: Configuration)
    extends Iterator[InternalRow] with Closeable with SparkAdapterSupport {

    protected val tableAvroSchema: Schema = new Schema.Parser().parse(tableSchema.avroSchemaStr)
    protected val requiredAvroSchema: Schema = new Schema.Parser().parse(requiredSchema.avroSchemaStr)
    protected val requiredFieldPosition: Seq[Int] = requiredSchema.structTypeSchema.map(f => tableAvroSchema.getField(f.name).pos()).toList
    protected val recordBuilder: GenericRecordBuilder= new GenericRecordBuilder(requiredAvroSchema)
    protected val deserializer: HoodieAvroDeserializer = sparkAdapter.createAvroDeserializer(requiredAvroSchema, requiredSchema.structTypeSchema)
    // NOTE: This have to stay lazy to make sure it's initialized only at the point where it's
    //       going to be used, since we modify `logRecords` before that and can do it any earlier
    protected lazy val logRecordsIterator: Iterator[(String, HoodieRecord[_ <: HoodieRecordPayload[_]])] = logRecords.iterator

    private var logScanner = HoodieMergeOnReadRDD.scanLog(split, tableAvroSchema, config)

    protected val logRecords = logScanner.getRecords.asScala
    protected val unsafeProjection: UnsafeProjection = UnsafeProjection.create(requiredSchema.structTypeSchema)

    protected var recordToLoad: InternalRow = _

    override def hasNext: Boolean =
      logRecordsIterator.hasNext && {
        val (_, record) = logRecordsIterator.next()
        val avroRecord = toScalaOption(record.getData.getInsertValue(tableAvroSchema, payloadProps))
        if (avroRecord.isEmpty) {
          // Record has been deleted, skipping
          this.hasNext
        } else {
          recordToLoad = deserialize(avroRecord.get.asInstanceOf[GenericRecord])
          true
        }
      }

    override final def next(): InternalRow = recordToLoad

    override def close(): Unit =
      if (logScanner != null) {
        try {
          logScanner.close()
        } finally {
          logScanner = null
        }
      }

    protected def deserialize(avroRecord: GenericRecord): InternalRow = {
      val requiredAvroRecord = AvroConversionUtils.buildAvroRecordBySchema(avroRecord, requiredAvroSchema,
        requiredFieldPosition, recordBuilder)
      val rowOpt = deserializer.deserialize(requiredAvroRecord)
      unsafeProjection(rowOpt.get.asInstanceOf[InternalRow])
    }
  }

  class SkipMergeIterator(split: HoodieMergeOnReadFileSplit,
                          baseFileIterator: Iterator[InternalRow],
                          config: Configuration) extends LogFileIterator(split, config) {

    override def hasNext: Boolean = {
      if (baseFileIterator.hasNext) {
        val curRow = baseFileIterator.next()
        recordToLoad = unsafeProjection(curRow)
        true
      } else {
        super[LogFileIterator].hasNext
      }
    }
  }
}

private object HoodieMergeOnReadRDD {
  val CONFIG_INSTANTIATION_LOCK = new Object()

  def scanLog(split: HoodieMergeOnReadFileSplit, logSchema: Schema, config: Configuration): HoodieMergedLogRecordScanner = {
    val fs = FSUtils.getFs(split.tablePath, config)
    val logFiles = split.logFiles.get

    if (HoodieTableMetadata.isMetadataTable(split.tablePath)) {
      val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build()
      val dataTableBasePath = getDataTableBasePathFromMetadataTable(split.tablePath)
      val metadataTable = new HoodieBackedTableMetadata(
        new HoodieLocalEngineContext(config), metadataConfig,
        dataTableBasePath,
        config.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP, HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))

      // NOTE: In case of Metadata Table partition path equates to partition name (since there's just one level
      //       of indirection among MT partitions)
      val relativePartitionPath = getRelativePartitionPath(new Path(split.tablePath), getPartitionPath(split))
      metadataTable.getLogRecordScanner(logFiles.asJava, relativePartitionPath).getLeft
    } else {
      val logRecordScannerBuilder = HoodieMergedLogRecordScanner.newBuilder()
        .withFileSystem(fs)
        .withBasePath(split.tablePath)
        .withLogFilePaths(split.logFiles.get.map(logFile => getFilePath(logFile.getPath)).asJava)
        .withReaderSchema(logSchema)
        .withLatestInstantTime(split.latestCommit)
        .withReadBlocksLazily(
          Try(config.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP,
            HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED).toBoolean)
            .getOrElse(false))
        .withReverseReader(false)
        .withBufferSize(
          config.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP,
            HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE))
        .withMaxMemorySizeInBytes(split.maxCompactionMemoryInBytes)
        .withSpillableMapBasePath(
          config.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP,
            HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))

      if (logFiles.nonEmpty) {
        logRecordScannerBuilder.withPartition(getRelativePartitionPath(new Path(split.tablePath), logFiles.head.getPath.getParent))
      }

      logRecordScannerBuilder.build()
    }
  }

  private def getPartitionPath(split: HoodieMergeOnReadFileSplit): Path = {
    // Determine partition path as an immediate parent folder of either
    //    - The base file
    //    - Some log file
    split.dataFile.map(baseFile => new Path(baseFile.filePath))
      .getOrElse(split.logFiles.get.head.getPath)
      .getParent
  }

  private def resolveAvroSchemaNullability(schema: Schema) = {
    AvroConversionUtils.resolveAvroTypeNullability(schema) match {
      case (nullable, _) => nullable
    }
  }
}
