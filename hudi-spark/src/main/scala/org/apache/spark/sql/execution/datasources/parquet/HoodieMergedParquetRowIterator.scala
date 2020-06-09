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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.log.{HoodieMergedLogRecordScanner, LogReaderUtils}
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit
import org.apache.parquet.hadoop.ParquetRecordReader
import org.apache.avro.Schema
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.StructType

import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.model.HoodieRecordPayload

import java.io.Closeable
import java.util
import scala.util.Try

/**
 * This class is the iterator for Hudi MOR table.
 * Log files are scanned on initialization.
 * This iterator will read the parquet file first and skip the record if it present in the log file.
 * Then read the log file.
 * Custom payload is not supported yet. This combining logic is matching with [OverwriteWithLatestAvroPayload]
 * @param rowReader ParquetRecordReader
 */
class HoodieMergedParquetRowIterator(private[this] var rowReader: ParquetRecordReader[UnsafeRow],
                                     private[this] val split: HoodieRealtimeFileSplit,
                                     private[this] val jobConf: JobConf) extends Iterator[UnsafeRow] with Closeable with Logging {
  private[this] var havePair = false
  private[this] var finished = false
  private[this] var parquetFinished = false
  private[this] var deltaRecordMap: util.Map[String,
    HoodieRecord[_ <: HoodieRecordPayload[_ <: HoodieRecordPayload[_ <: AnyRef]]]] = _
  private[this] var deltaRecordKeys: util.Set[String] = _
  private[this] var deltaIter: util.Iterator[String] = _
  private[this] var avroSchema: Schema = _
  private[this] var sparkTypes: StructType = _
  private[this] var converter: AvroDeserializer = _

  // The rowReader has to be initialized after the Iterator constructed
  // So we need to initialize the Iterator after rowReader initialized
  def init(): Unit = {
    deltaRecordMap = getMergedLogRecordScanner.getRecords
    deltaRecordKeys = deltaRecordMap.keySet()
    deltaIter = deltaRecordKeys.iterator()
    avroSchema = getLogAvroSchema()
    sparkTypes = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
    converter = new AvroDeserializer(avroSchema, sparkTypes)
  }

  override def hasNext: Boolean = {
    if (!parquetFinished) {
      //org.apache.spark.sql.execution.datasources.FileScanRDD.getNext() call this hasNext but with havePair = true
      //so it won't trigger reading the next row
      //but next() in this class use havePair = false to trigger reading next row
      if (!parquetFinished && !havePair) {
        // check if next row exist and read next row in rowReader
        parquetFinished = !rowReader.nextKeyValue
        // skip if record is in delta map
        while (!parquetFinished && skipCurrentValue(rowReader.getCurrentValue)) {
          parquetFinished = !rowReader.nextKeyValue
        }
        // set back to true for FileScanRDD.getNext() to call
        havePair = !finished
      }
      !finished
    } else {
      if (deltaIter.hasNext) {
        !finished
      }
      else {
        finished = true
        // Close and release the reader here; close() will also be called when the task
        // completes, but for tasks that read from many files, it helps to release the
        // resources early.
        logInfo("closing reader")
        close()
        !finished
      }
    }
  }

  override def next(): UnsafeRow = {
    if (!parquetFinished) {
      if (!hasNext) {
        throw new java.util.NoSuchElementException("End of stream")
      }
      havePair = false
      rowReader.getCurrentValue
    } else {
      getLogRecord
    }
  }

  override def close(): Unit = {
    if (rowReader != null) {
      try {
        rowReader.close()
      } finally {
        rowReader = null
      }
    }
  }

  // While reading the parquet file, skip the record if it presented in the log file already.
  private def skipCurrentValue(currentValue: UnsafeRow): Boolean = {
    val curKey = currentValue.getString(HOODIE_RECORD_KEY_COL_POS)
    if (deltaRecordKeys.contains(curKey)) {
      logInfo(s"$curKey is in the delta map, skipping")
      true
    } else {
      logInfo(s"$curKey is NOT in the delta map, reading")
      false
    }
  }

  // TODO: Directly deserialize to UnsafeRow
  private def toUnsafeRow(row: InternalRow, schema: StructType): UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    converter.apply(row)
  }

  private def getLogRecord: UnsafeRow = {
    val curRecord = deltaRecordMap.get(deltaIter.next()).getData.getInsertValue(avroSchema).get()
    // Convert Avro GenericRecord to InternalRow
    val curRow = converter.deserialize(curRecord).asInstanceOf[InternalRow]
    // Convert InternalRow to UnsafeRow
    toUnsafeRow(curRow, sparkTypes)
  }

  private def getMergedLogRecordScanner: HoodieMergedLogRecordScanner = {
    new HoodieMergedLogRecordScanner(
      FSUtils.getFs(split.getPath().toString(), jobConf),
      split.getBasePath(),
      split.getDeltaLogPaths(),
      getLogAvroSchema(), // currently doesn't support custom payload, use schema from the log file as default
      split.getMaxCommitTime(),
      getMaxCompactionMemoryInBytes,
      Try(jobConf.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP, HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED).toBoolean).getOrElse(false),
      false,
      jobConf.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP, HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE),
      jobConf.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP, HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))
  }

  private def getMaxCompactionMemoryInBytes: Long = { // jobConf.getMemoryForMapTask() returns in MB
    Math.ceil(jobConf.get(
      HoodieRealtimeConfig.COMPACTION_MEMORY_FRACTION_PROP,
      HoodieRealtimeConfig.DEFAULT_COMPACTION_MEMORY_FRACTION).toDouble * jobConf.getMemoryForMapTask * 1024 * 1024L).toLong
  }

  private def getLogAvroSchema(): Schema = {
    LogReaderUtils.readLatestSchemaFromLogFiles(split.getBasePath, split.getDeltaLogPaths, jobConf)
  }
}
