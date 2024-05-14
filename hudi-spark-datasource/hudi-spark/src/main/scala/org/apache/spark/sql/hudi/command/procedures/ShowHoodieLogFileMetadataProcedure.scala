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

import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.HoodieLogBlock.{HeaderMetadataType, HoodieLogBlockType}
import org.apache.hudi.common.table.log.block.{HoodieCorruptBlock, HoodieDataBlock}
import org.apache.hudi.storage.StoragePath

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.parquet.avro.AvroSchemaConverter
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util.Objects
import java.util.concurrent.atomic.AtomicInteger
import java.util.function.Supplier
import scala.collection.JavaConverters.{asScalaBufferConverter, asScalaIteratorConverter, mapAsScalaMapConverter}

class ShowHoodieLogFileMetadataProcedure extends BaseProcedure with ProcedureBuilder {
  override def parameters: Array[ProcedureParameter] = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "log_file_path_pattern", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, 10)
  )

  override def outputType: StructType = StructType(Array[StructField](
    StructField("instant_time", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("record_count", DataTypes.IntegerType, nullable = true, Metadata.empty),
    StructField("block_type", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("header_metadata", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("footer_metadata", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  override def call(args: ProcedureArgs): Seq[Row] = {
    checkArgs(parameters, args)
    val table = getArgValueOrDefault(args, parameters(0))
    val logFilePathPattern: String = getArgValueOrDefault(args, parameters(1)).get.asInstanceOf[String]
    val limit: Int = getArgValueOrDefault(args, parameters(2)).get.asInstanceOf[Int]
    val basePath = getBasePath(table)
    val storage = createMetaClient(jsc, basePath).getStorage
    val logFilePaths = FSUtils.getGlobStatusExcludingMetaFolder(storage, new StoragePath(logFilePathPattern)).iterator().asScala
      .map(_.getPath.toString).toList
    val commitCountAndMetadata =
      new java.util.HashMap[String, java.util.List[(HoodieLogBlockType, (java.util.Map[HeaderMetadataType, String], java.util.Map[HeaderMetadataType, String]), Int)]]()
    var numCorruptBlocks = 0
    var dummyInstantTimeCount = 0
    logFilePaths.foreach {
      logFilePath => {
        val statuses = storage.listDirectEntries(new StoragePath(logFilePath))
        val schema = TableSchemaResolver.readSchemaFromLogFile(storage, new StoragePath(logFilePath))
        val reader = HoodieLogFormat.newReader(storage, new HoodieLogFile(statuses.get(0).getPath), schema)

        // read the avro blocks
        while (reader.hasNext) {
          val block = reader.next()
          val recordCount = new AtomicInteger(0)
          var instantTime: String = null
          if (block.isInstanceOf[HoodieCorruptBlock]) {
            try {
              instantTime = block.getLogBlockHeader.get(HeaderMetadataType.INSTANT_TIME)
              if (null == instantTime) {
                throw new java.lang.Exception("Invalid instant time " + instantTime)
              }
            } catch {
              case _: java.lang.Exception =>
                numCorruptBlocks = numCorruptBlocks + 1;
                instantTime = "corrupt_block_" + numCorruptBlocks
            }
          } else {
            instantTime = block.getLogBlockHeader.get(HeaderMetadataType.INSTANT_TIME)
            if (null == instantTime) {
              dummyInstantTimeCount = dummyInstantTimeCount + 1
              instantTime = "dummy_instant_time_" + dummyInstantTimeCount
            }
            block match {
              case dataBlock: HoodieDataBlock =>
                val recordItr = dataBlock.getRecordIterator(HoodieRecordType.AVRO)
                recordItr.asScala.foreach(_ => recordCount.incrementAndGet())
                recordItr.close()
            }
          }
          if (commitCountAndMetadata.containsKey(instantTime)) {
            val list = commitCountAndMetadata.get(instantTime)
            list.add((block.getBlockType, (block.getLogBlockHeader, block.getLogBlockFooter), recordCount.get()))
          } else {
            val list = new java.util.ArrayList[(HoodieLogBlockType, (java.util.Map[HeaderMetadataType, String], java.util.Map[HeaderMetadataType, String]), Int)]
            list.add(block.getBlockType, (block.getLogBlockHeader, block.getLogBlockFooter), recordCount.get())
            commitCountAndMetadata.put(instantTime, list)
          }
        }
        reader.close()
      }
    }
    val rows = new java.util.ArrayList[Row]
    val objectMapper = new ObjectMapper()
    commitCountAndMetadata.asScala.foreach {
      case (instantTime, values) =>
        values.asScala.foreach {
          tuple3 =>
            rows.add(Row(
              instantTime,
              tuple3._3,
              tuple3._1.toString,
              objectMapper.writeValueAsString(tuple3._2._1),
              objectMapper.writeValueAsString(tuple3._2._2)
            ))
        }
    }
    rows.stream().limit(limit).toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new ShowHoodieLogFileMetadataProcedure
}

object ShowHoodieLogFileMetadataProcedure {
  val NAME = "show_logfile_metadata"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get(): ProcedureBuilder = new ShowHoodieLogFileMetadataProcedure()
  }
}
