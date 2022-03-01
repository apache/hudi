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
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.HoodieDataSourceHelper._
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner
import org.apache.hudi.config.HoodiePayloadConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.{HoodieAvroDeserializer, HoodieAvroSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}

import java.io.Closeable
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

case class HoodieMergeOnReadPartition(index: Int, split: HoodieMergeOnReadFileSplit) extends Partition

class HoodieMergeOnReadRDD(@transient sc: SparkContext,
                           @transient config: Configuration,
                           fullSchemaFileReader: PartitionedFile => Iterator[InternalRow],
                           requiredSchemaFileReader: PartitionedFile => Iterator[InternalRow],
                           tableState: HoodieMergeOnReadTableState)
  extends RDD[InternalRow](sc, Nil) {

  private val confBroadcast = sc.broadcast(new SerializableWritable(config))
  private val preCombineField = tableState.preCombineField
  private val recordKeyFieldOpt = tableState.recordKeyFieldOpt
  private val payloadProps = if (preCombineField.isDefined) {
    HoodiePayloadConfig.newBuilder
      .withPayloadOrderingField(preCombineField.get)
      .build.getProps
  } else {
    new Properties()
  }

  private val requiredSchema = tableState.requiredStructSchema

  private val requiredFieldPosition = HoodieSparkUtils.collectFieldIndexes(requiredSchema,
    tableState.tableStructSchema
  )

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val mergeOnReadPartition = split.asInstanceOf[HoodieMergeOnReadPartition]
    val iter = mergeOnReadPartition.split match {
      case dataFileOnlySplit if dataFileOnlySplit.logPaths.isEmpty =>
        val rows = requiredSchemaFileReader(dataFileOnlySplit.dataFile.get)
        extractRequiredSchema(rows, requiredSchema, requiredFieldPosition)
      case logFileOnlySplit if logFileOnlySplit.dataFile.isEmpty =>
        logFileIterator(logFileOnlySplit, getConfig)
      case skipMergeSplit if skipMergeSplit.mergeType
        .equals(DataSourceReadOptions.REALTIME_SKIP_MERGE_OPT_VAL) =>
        skipMergeFileIterator(
          skipMergeSplit,
          requiredSchemaFileReader(skipMergeSplit.dataFile.get),
          getConfig
        )
      case payloadCombineSplit if payloadCombineSplit.mergeType
        .equals(DataSourceReadOptions.REALTIME_PAYLOAD_COMBINE_OPT_VAL) =>
        payloadCombineFileIterator(
          payloadCombineSplit,
          fullSchemaFileReader(payloadCombineSplit.dataFile.get),
          getConfig
        )
      case _ => throw new HoodieException(s"Unable to select an Iterator to read the Hoodie MOR File Split for " +
        s"file path: ${mergeOnReadPartition.split.dataFile.get.filePath}" +
        s"log paths: ${mergeOnReadPartition.split.logPaths.toString}" +
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

  override protected def getPartitions: Array[Partition] = {
    tableState
      .hoodieRealtimeFileSplits
      .zipWithIndex
      .map(file => HoodieMergeOnReadPartition(file._2, file._1)).toArray
  }

  private def getConfig: Configuration = {
    val conf = confBroadcast.value.value
    HoodieMergeOnReadRDD.CONFIG_INSTANTIATION_LOCK.synchronized {
      new Configuration(conf)
    }
  }

  private def logFileIterator(split: HoodieMergeOnReadFileSplit,
                              config: Configuration): Iterator[InternalRow] =
    new Iterator[InternalRow] with Closeable {
      private val tableAvroSchema = new Schema.Parser().parse(tableState.tableAvroSchema)
      private val requiredAvroSchema = new Schema.Parser().parse(tableState.requiredAvroSchema)
      private val recordBuilder = new GenericRecordBuilder(requiredAvroSchema)
      private val deserializer = HoodieAvroDeserializer(requiredAvroSchema, tableState.requiredStructSchema)
      private val unsafeProjection = UnsafeProjection.create(tableState.requiredStructSchema)
      private var logScanner = HoodieMergeOnReadRDD.scanLog(split, tableAvroSchema, config)
      private val logRecords = logScanner.getRecords
      private val logRecordsKeyIterator = logRecords.keySet().iterator().asScala

      private var recordToLoad: InternalRow = _

      override def hasNext: Boolean = {
        if (logRecordsKeyIterator.hasNext) {
          val curAvrokey = logRecordsKeyIterator.next()
          val curAvroRecord = logRecords.get(curAvrokey).getData.getInsertValue(tableAvroSchema, payloadProps)
          if (!curAvroRecord.isPresent) {
            // delete record found, skipping
            this.hasNext
          } else {
            val requiredAvroRecord = AvroConversionUtils
              .buildAvroRecordBySchema(curAvroRecord.get(), requiredAvroSchema, requiredFieldPosition, recordBuilder)
            recordToLoad = unsafeProjection(deserializer.deserializeData(requiredAvroRecord).asInstanceOf[InternalRow])
            true
          }
        } else {
          false
        }
      }

      override def next(): InternalRow = {
        recordToLoad
      }

      override def close(): Unit = {
        if (logScanner != null) {
          try {
            logScanner.close()
          } finally {
            logScanner = null
          }
        }
      }
    }

  private def skipMergeFileIterator(split: HoodieMergeOnReadFileSplit,
                                    baseFileIterator: Iterator[InternalRow],
                                    config: Configuration): Iterator[InternalRow] =
    new Iterator[InternalRow] with Closeable {
      private val tableAvroSchema = new Schema.Parser().parse(tableState.tableAvroSchema)
      private val requiredAvroSchema = new Schema.Parser().parse(tableState.requiredAvroSchema)
      private val recordBuilder = new GenericRecordBuilder(requiredAvroSchema)
      private val deserializer = HoodieAvroDeserializer(requiredAvroSchema, tableState.requiredStructSchema)
      private val unsafeProjection = UnsafeProjection.create(tableState.requiredStructSchema)
      private var logScanner = HoodieMergeOnReadRDD.scanLog(split, tableAvroSchema, config)
      private val logRecords = logScanner.getRecords
      private val logRecordsKeyIterator = logRecords.keySet().iterator().asScala

      private var recordToLoad: InternalRow = _

      @scala.annotation.tailrec
      override def hasNext: Boolean = {
        if (baseFileIterator.hasNext) {
          val curRow = baseFileIterator.next()
          recordToLoad = unsafeProjection(createInternalRowWithSchema(curRow, requiredSchema, requiredFieldPosition))
          true
        } else {
          if (logRecordsKeyIterator.hasNext) {
            val curAvrokey = logRecordsKeyIterator.next()
            val curAvroRecord = logRecords.get(curAvrokey).getData.getInsertValue(tableAvroSchema, payloadProps)
            if (!curAvroRecord.isPresent) {
              // delete record found, skipping
              this.hasNext
            } else {
              val requiredAvroRecord = AvroConversionUtils
                .buildAvroRecordBySchema(curAvroRecord.get(), requiredAvroSchema, requiredFieldPosition, recordBuilder)
              recordToLoad = unsafeProjection(deserializer.deserializeData(requiredAvroRecord).asInstanceOf[InternalRow])
              true
            }
          } else {
            false
          }
        }
      }

      override def next(): InternalRow = {
        recordToLoad
      }

      override def close(): Unit = {
        if (logScanner != null) {
          try {
            logScanner.close()
          } finally {
            logScanner = null
          }
        }
      }
    }

  private def payloadCombineFileIterator(split: HoodieMergeOnReadFileSplit,
                                         baseFileIterator: Iterator[InternalRow],
                                         config: Configuration): Iterator[InternalRow] =
    new Iterator[InternalRow] with Closeable {
      private val tableAvroSchema = new Schema.Parser().parse(tableState.tableAvroSchema)
      private val requiredAvroSchema = new Schema.Parser().parse(tableState.requiredAvroSchema)
      private val serializer = HoodieAvroSerializer(tableState.tableStructSchema, tableAvroSchema, false)
      private val requiredDeserializer = HoodieAvroDeserializer(requiredAvroSchema, tableState.requiredStructSchema)
      private val recordBuilder = new GenericRecordBuilder(requiredAvroSchema)
      private val unsafeProjection = UnsafeProjection.create(tableState.requiredStructSchema)
      private var logScanner = HoodieMergeOnReadRDD.scanLog(split, tableAvroSchema, config)
      private val logRecords = logScanner.getRecords
      private val logRecordsKeyIterator = logRecords.keySet().iterator().asScala
      private val keyToSkip = mutable.Set.empty[String]
      private val recordKeyPosition = if (recordKeyFieldOpt.isEmpty) HOODIE_RECORD_KEY_COL_POS else tableState.tableStructSchema.fieldIndex(recordKeyFieldOpt.get)

      private var recordToLoad: InternalRow = _

      @scala.annotation.tailrec
      override def hasNext: Boolean = {
        if (baseFileIterator.hasNext) {
          val curRow = baseFileIterator.next()
          val curKey = curRow.getString(recordKeyPosition)
          if (logRecords.containsKey(curKey)) {
            // duplicate key found, merging
            keyToSkip.add(curKey)
            val mergedAvroRecord = mergeRowWithLog(curRow, curKey)
            if (!mergedAvroRecord.isPresent) {
              // deleted
              this.hasNext
            } else {
              // load merged record as InternalRow with required schema
              val requiredAvroRecord = AvroConversionUtils
                .buildAvroRecordBySchema(
                  mergedAvroRecord.get(),
                  requiredAvroSchema,
                  requiredFieldPosition,
                  recordBuilder
                )
              recordToLoad = unsafeProjection(requiredDeserializer
                .deserializeData(requiredAvroRecord).asInstanceOf[InternalRow])
              true
            }
          } else {
            // No merge needed, load current row with required schema
            recordToLoad = unsafeProjection(createInternalRowWithSchema(curRow, requiredSchema, requiredFieldPosition))
            true
          }
        } else {
          if (logRecordsKeyIterator.hasNext) {
            val curKey = logRecordsKeyIterator.next()
            if (keyToSkip.contains(curKey)) {
              this.hasNext
            } else {
              val insertAvroRecord = logRecords.get(curKey).getData.getInsertValue(tableAvroSchema, payloadProps)
              if (!insertAvroRecord.isPresent) {
                // stand alone delete record, skipping
                this.hasNext
              } else {
                val requiredAvroRecord = AvroConversionUtils
                  .buildAvroRecordBySchema(
                    insertAvroRecord.get(),
                    requiredAvroSchema,
                    requiredFieldPosition,
                    recordBuilder
                  )
                recordToLoad = unsafeProjection(requiredDeserializer
                  .deserializeData(requiredAvroRecord).asInstanceOf[InternalRow])
                true
              }
            }
          } else {
            false
          }
        }
      }

      override def next(): InternalRow = recordToLoad

      override def close(): Unit = {
        if (logScanner != null) {
          try {
            logScanner.close()
          } finally {
            logScanner = null
          }
        }
      }

      private def mergeRowWithLog(curRow: InternalRow, curKey: String) = {
        val historyAvroRecord = serializer.serialize(curRow).asInstanceOf[GenericRecord]
        logRecords.get(curKey).getData.combineAndGetUpdateValue(
          historyAvroRecord, tableAvroSchema, payloadProps)
      }
    }
}

private object HoodieMergeOnReadRDD {
  val CONFIG_INSTANTIATION_LOCK = new Object()

  def scanLog(split: HoodieMergeOnReadFileSplit, logSchema: Schema, config: Configuration): HoodieMergedLogRecordScanner = {
    val fs = FSUtils.getFs(split.tablePath, config)
    val partitionPath: String = if (split.logPaths.isEmpty || split.logPaths.get.asJava.isEmpty) {
      null
    } else {
      new Path(split.logPaths.get.asJava.get(0)).getParent.getName
    }
    val logRecordScannerBuilder = HoodieMergedLogRecordScanner.newBuilder()
      .withFileSystem(fs)
      .withBasePath(split.tablePath)
      .withLogFilePaths(split.logPaths.get.asJava)
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
    if (partitionPath != null) {
      logRecordScannerBuilder.withPartition(partitionPath)
    }
    logRecordScannerBuilder.build()
  }
}
