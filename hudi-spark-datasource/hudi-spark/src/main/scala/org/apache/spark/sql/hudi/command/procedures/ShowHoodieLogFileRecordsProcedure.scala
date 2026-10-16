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
import org.apache.hudi.common.model.{HoodieLogFile, HoodieRecordPayload}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.log.{HoodieLogFormat, HoodieMergedLogRecordScanner}
import org.apache.hudi.common.table.log.block.HoodieDataBlock
import org.apache.hudi.common.util.ValidationUtils
import org.apache.hudi.io.util.FileIOUtils
import org.apache.hudi.storage.StoragePath

import org.apache.avro.generic.IndexedRecord
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.Objects
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowHoodieLogFileRecordsProcedure extends BaseProcedure with ProcedureBuilder {
  /** Rows buffered before the filter is evaluated, so the driver heap does not grow with the log files. */
  private val FILTER_BATCH_SIZE = 1024

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
    // `limit` bounds how many records are read out of the log files, so a filter cannot simply be applied
    // to the first `limit`. Rather than lifting the bound and buffering every record on the driver heap,
    // rows are filtered in batches and only the matches are kept, so scanning stops once `limit` of them
    // have been found. Heap stays bounded by the batch plus the matches, whatever the log files hold.
    // With no filter applyFilter is a passthrough, so a batch of one reproduces the previous bound exactly.
    val batchSize = if (hasFilter(filter)) FILTER_BATCH_SIZE else 1
    val matched = new java.util.ArrayList[Row]
    val batch = new java.util.ArrayList[Row](batchSize)

    def flushBatch(): Unit = {
      if (!batch.isEmpty) {
        matched.addAll(applyFilter(batch.asScala.toSeq, filter, outputType).asJava)
        batch.clear()
      }
    }

    def enoughMatches: Boolean = matched.size() >= limit

    def offer(record: IndexedRecord): Unit = {
      if (!enoughMatches) {
        batch.add(Row(record.toString))
        if (batch.size() >= batchSize) {
          flushBatch()
        }
      }
    }

    val client = createMetaClient(jsc, basePath)
    val storage = client.getStorage
    val logFilePaths = FSUtils.getGlobStatusExcludingMetaFolder(storage, new StoragePath(logFilePathPattern)).iterator().asScala
      .map(_.getPath.toString).toList
    ValidationUtils.checkArgument(logFilePaths.nonEmpty, "There is no log file")
    if (merge) {
      val schema = Objects.requireNonNull(TableSchemaResolver.readSchemaFromLogFile(client, new StoragePath(logFilePaths.last)))
      val scanner = HoodieMergedLogRecordScanner.newBuilder
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(logFilePaths.asJava)
        .withReaderSchema(schema)
        .withLatestInstantTime(client.getActiveTimeline.getCommitAndReplaceTimeline.lastInstant.get.requestedTime)
        .withReverseReader(java.lang.Boolean.parseBoolean(HoodieReaderConfig.COMPACTION_REVERSE_LOG_READ_ENABLE.defaultValue))
        .withBufferSize(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.defaultValue)
        .withMaxMemorySizeInBytes(HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES)
        .withSpillableMapBasePath(FileIOUtils.getDefaultSpillableMapBasePath)
        .withDiskMapType(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue)
        .withBitCaskDiskMapCompressionEnabled(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue)
        .build
      scanner.asScala.takeWhile(_ => !enoughMatches).foreach(hoodieRecord => {
        offer(hoodieRecord.getData.asInstanceOf[HoodieRecordPayload[_]].getInsertValue(schema.toAvroSchema).get())
      })
    } else {
      logFilePaths.toStream.takeWhile(_ => !enoughMatches).foreach {
        logFilePath => {
          val schema = Objects.requireNonNull(TableSchemaResolver.readSchemaFromLogFile(client, new StoragePath(logFilePath)))
          val reader = HoodieLogFormat.newReader(client, new HoodieLogFile(logFilePath), schema)
          while (reader.hasNext && !enoughMatches) {
            val block = reader.next()
            block match {
              case dataBlock: HoodieDataBlock =>
                val recordItr = dataBlock.getRecordIterator(HoodieRecordType.AVRO)
                recordItr.asScala.takeWhile(_ => !enoughMatches).foreach(record => {
                  offer(record.getData.asInstanceOf[IndexedRecord])
                })
                recordItr.close()
            }
          }
          reader.close()
        }
      }
    }
    flushBatch()
    matched.asScala.toSeq.take(limit)
  }

  override def build: Procedure = new ShowHoodieLogFileRecordsProcedure
}

object ShowHoodieLogFileRecordsProcedure {
  val NAME = "show_logfile_records"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowHoodieLogFileRecordsProcedure()
  }
}
