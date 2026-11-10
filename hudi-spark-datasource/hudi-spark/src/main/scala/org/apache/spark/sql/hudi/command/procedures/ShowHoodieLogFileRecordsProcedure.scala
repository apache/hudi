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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMemoryConfig, HoodieReaderConfig}
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.log.{HoodieLogFormat, HoodieMergedLogRecordScanner}
import org.apache.hudi.common.table.log.block.HoodieDataBlock
import org.apache.hudi.common.table.read.DeleteContext
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator
import org.apache.hudi.common.util.ValidationUtils
import org.apache.hudi.io.util.FileIOUtils
import org.apache.hudi.storage.StoragePath

import org.apache.avro.generic.{GenericData, IndexedRecord}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowHoodieLogFileRecordsProcedure extends BaseProcedure with ProcedureBuilder {
  override def parameters: Array[ProcedureParameter] = Array[ProcedureParameter](
    ProcedureParameter.optional(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "path", DataTypes.StringType),
    ProcedureParameter.required(2, "log_file_path_pattern", DataTypes.StringType),
    ProcedureParameter.optional(3, "merge", DataTypes.BooleanType, false),
    ProcedureParameter.optional(4, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(5, "filter", DataTypes.StringType, "")
  )

  override def outputType: StructType = StructType(Array[StructField](
    StructField("records", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def call(args: ProcedureArgs): Seq[Row] = {
    val table = getArgValueOrDefault(args, parameters(0))
    val path = getArgValueOrDefault(args, parameters(1))
    val basePath = getBasePath(table, path)
    checkArgs(parameters, args)
    val logFilePathPattern: String = getArgValueOrDefault(args, parameters(2)).get.asInstanceOf[String]
    val merge: Boolean = getArgValueOrDefault(args, parameters(3)).get.asInstanceOf[Boolean]
    val limit: Int = getArgValueOrDefault(args, parameters(4)).get.asInstanceOf[Int]
    val filter = getArgValueOrDefault(args, parameters(5)).get.asInstanceOf[String]

    validateFilter(filter, outputType)
    val client = createMetaClient(jsc, basePath)
    val storage = client.getStorage
    val logFilePaths = FSUtils.getGlobStatusExcludingMetaFolder(storage, new StoragePath(logFilePathPattern)).iterator().asScala
      .map(_.getPath.toString).toList
    ValidationUtils.checkArgument(logFilePaths.nonEmpty, "There is no log file")
    val allRecords: java.util.List[IndexedRecord] = new java.util.ArrayList[IndexedRecord]
    if (merge) {
      // A log file with only delete or command blocks has no schema, so read it from the last globbed file that has one.
      val readerSchema = logFilePaths.reverseIterator
        .map(logFilePath => TableSchemaResolver.readSchemaFromLogFile(client, new StoragePath(logFilePath)))
        .find(_ != null)
      // When no log file has a data block there is nothing to merge.
      readerSchema.foreach(schema => addMergedRecords(client, basePath, logFilePaths, schema, limit, allRecords))
    } else {
      logFilePaths.toStream.takeWhile(_ => allRecords.size() < limit).foreach {
        logFilePath => {
          // Null for a log file with only delete or command blocks, which are skipped below.
          val schema = TableSchemaResolver.readSchemaFromLogFile(client, new StoragePath(logFilePath))
          val reader = HoodieLogFormat.newReader(client, new HoodieLogFile(logFilePath), schema)
          while (reader.hasNext) {
            val block = reader.next()
            block match {
              case dataBlock: HoodieDataBlock =>
                val recordItr = dataBlock.getRecordIterator(HoodieRecordType.AVRO)
                recordItr.asScala.foreach(record => {
                  if (allRecords.size() < limit) {
                    allRecords.add(record.getData.asInstanceOf[IndexedRecord])
                  }
                })
                recordItr.close()
              case _ =>
            }
          }
          reader.close()
        }
      }
    }
    val rows: java.util.List[Row] = new java.util.ArrayList[Row](allRecords.size())
    allRecords.asScala.foreach(record => {
      // GenericData reads every field, so a lazily decoded record prints its values rather than its raw bytes.
      rows.add(Row(GenericData.get.toString(record)))
    })
    val results = rows.asScala.toSeq
    applyFilter(results, filter, outputType)
  }

  private def addMergedRecords(client: HoodieTableMetaClient, basePath: String, logFilePaths: List[String],
                               schema: HoodieSchema, limit: Int, allRecords: java.util.List[IndexedRecord]): Unit = {
    // Bound the scan by the latest completed instant of any write action, so an uncompacted MOR table
    // (deltacommits only) and log blocks written after the last compaction are both read.
    val latestInstantTime = client.getActiveTimeline.getCommitsTimeline.filterCompletedInstants.lastInstant
      .map[String](instant => instant.requestedTime)
      .orElse(HoodieInstantTimeGenerator.getCurrentInstantTimeStr)
    val scanner = HoodieMergedLogRecordScanner.newBuilder
      .withStorage(client.getStorage)
      .withBasePath(basePath)
      .withLogFilePaths(logFilePaths.asJava)
      .withReaderSchema(schema)
      .withLatestInstantTime(latestInstantTime)
      .withReverseReader(java.lang.Boolean.parseBoolean(HoodieReaderConfig.COMPACTION_REVERSE_LOG_READ_ENABLE.defaultValue))
      .withBufferSize(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.defaultValue)
      .withMaxMemorySizeInBytes(HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES)
      .withSpillableMapBasePath(FileIOUtils.getDefaultSpillableMapBasePath)
      .withDiskMapType(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue)
      .withBitCaskDiskMapCompressionEnabled(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue)
      .build
    try {
      val props = client.getTableConfig.getProps
      val deleteContext = new DeleteContext(props, schema).withReaderSchema(schema)
      scanner.asScala.foreach(hoodieRecord => {
        // A key whose merged result is a delete, from a delete block or a soft delete, is not shown.
        if (allRecords.size() < limit && !hoodieRecord.isDelete(deleteContext, props)) {
          val record = hoodieRecord.toIndexedRecord(schema, props)
          if (record.isPresent) {
            allRecords.add(record.get.getData)
          }
        }
      })
    } finally {
      scanner.close()
    }
  }

  override def build: Procedure = new ShowHoodieLogFileRecordsProcedure
}

object ShowHoodieLogFileRecordsProcedure {
  val NAME = "show_logfile_records"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowHoodieLogFileRecordsProcedure()
  }
}
