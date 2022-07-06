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

import com.google.common.collect.Lists
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.fs.Path
import org.apache.hudi.common.config.HoodieCommonConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.table.log.block.HoodieDataBlock
import org.apache.hudi.common.table.log.{HoodieLogFormat, HoodieMergedLogRecordScanner}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.util.ValidationUtils
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieMemoryConfig}
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.Objects
import java.util.function.Supplier
import scala.collection.JavaConverters._

class ShowHoodieLogFileRecordsProcedure extends BaseProcedure with ProcedureBuilder {
  override def parameters: Array[ProcedureParameter] = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.required(1, "log_file_path_pattern", DataTypes.StringType, None),
    ProcedureParameter.optional(2, "merge", DataTypes.BooleanType, false),
    ProcedureParameter.optional(3, "limit", DataTypes.IntegerType, 10)
  )

  override def outputType: StructType = StructType(Array[StructField](
    StructField("records", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def call(args: ProcedureArgs): Seq[Row] = {
    checkArgs(parameters, args)
    val table = getArgValueOrDefault(args, parameters(0))
    val logFilePathPattern: String = getArgValueOrDefault(args, parameters(1)).get.asInstanceOf[String]
    val merge: Boolean = getArgValueOrDefault(args, parameters(2)).get.asInstanceOf[Boolean]
    val limit: Int = getArgValueOrDefault(args, parameters(3)).get.asInstanceOf[Int]
    val basePath = getBasePath(table)
    val client = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val fs = client.getFs
    val logFilePaths = FSUtils.getGlobStatusExcludingMetaFolder(fs, new Path(logFilePathPattern)).iterator().asScala
      .map(_.getPath.toString).toList
    ValidationUtils.checkArgument(logFilePaths.nonEmpty, "There is no log file")
    val converter = new AvroSchemaConverter()
    val allRecords: java.util.List[IndexedRecord] = Lists.newArrayList()
    if (merge) {
      val schema = converter.convert(Objects.requireNonNull(TableSchemaResolver.readSchemaFromLogFile(fs, new Path(logFilePaths.last))))
      val scanner = HoodieMergedLogRecordScanner.newBuilder
        .withFileSystem(fs)
        .withBasePath(basePath)
        .withLogFilePaths(logFilePaths.asJava)
        .withReaderSchema(schema)
        .withLatestInstantTime(client.getActiveTimeline.getCommitTimeline.lastInstant.get.getTimestamp)
        .withReadBlocksLazily(java.lang.Boolean.parseBoolean(HoodieCompactionConfig.COMPACTION_LAZY_BLOCK_READ_ENABLE.defaultValue))
        .withReverseReader(java.lang.Boolean.parseBoolean(HoodieCompactionConfig.COMPACTION_REVERSE_LOG_READ_ENABLE.defaultValue))
        .withBufferSize(HoodieMemoryConfig.MAX_DFS_STREAM_BUFFER_SIZE.defaultValue)
        .withMaxMemorySizeInBytes(HoodieMemoryConfig.DEFAULT_MAX_MEMORY_FOR_SPILLABLE_MAP_IN_BYTES)
        .withSpillableMapBasePath(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.defaultValue)
        .withDiskMapType(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.defaultValue)
        .withBitCaskDiskMapCompressionEnabled(HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED.defaultValue)
        .build
      scanner.asScala.foreach(hoodieRecord => {
        val record = hoodieRecord.getData.getInsertValue(schema).get()
        if (allRecords.size() < limit) {
          allRecords.add(record)
        }
      })
    } else {
      logFilePaths.toStream.takeWhile(_ => allRecords.size() < limit).foreach {
        logFilePath => {
          val schema = converter.convert(Objects.requireNonNull(TableSchemaResolver.readSchemaFromLogFile(fs, new Path(logFilePath))))
          val reader = HoodieLogFormat.newReader(fs, new HoodieLogFile(logFilePath), schema)
          while (reader.hasNext) {
            val block = reader.next()
            block match {
              case dataBlock: HoodieDataBlock =>
                val recordItr = dataBlock.getRecordIterator
                recordItr.asScala.foreach(record => {
                  if (allRecords.size() < limit) {
                    allRecords.add(record)
                  }
                })
                recordItr.close()
            }
          }
          reader.close()
        }
      }
    }
    val rows: java.util.List[Row] = Lists.newArrayList()
    allRecords.asScala.foreach(record => {
      rows.add(Row(record.toString))
    })
    rows.asScala
  }

  override def build: Procedure = new ShowHoodieLogFileRecordsProcedure
}

object ShowHoodieLogFileRecordsProcedure {
  val NAME = "show_logfile_records"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowHoodieLogFileRecordsProcedure()
  }
}
