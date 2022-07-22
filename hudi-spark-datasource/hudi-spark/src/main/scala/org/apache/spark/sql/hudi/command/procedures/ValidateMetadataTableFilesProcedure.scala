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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.HoodieTimer
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.Collections
import java.util.function.Supplier
import scala.collection.JavaConversions._
import scala.collection.JavaConverters.asScalaIteratorConverter

class ValidateMetadataTableFilesProcedure() extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType, None),
    ProcedureParameter.optional(1, "verbose", DataTypes.BooleanType, false)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_name", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("is_present_in_fs", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("is_resent_in_metadata", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("fs_size", DataTypes.LongType, nullable = true, Metadata.empty),
    StructField("metadata_size", DataTypes.LongType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0))
    val verbose = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[Boolean]

    val basePath = getBasePath(table)
    val metaClient = HoodieTableMetaClient.builder.setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build
    val config = HoodieMetadataConfig.newBuilder.enable(true).build
    val metadataReader = new HoodieBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getHadoopConf),
      config, basePath, "/tmp")

    if (!metadataReader.enabled){
      throw new HoodieException(s"Metadata Table not enabled/initialized.")
    }

    val fsConfig = HoodieMetadataConfig.newBuilder.enable(false).build
    val fsMetaReader = new HoodieBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getHadoopConf),
      fsConfig, basePath, "/tmp")

    val timer = new HoodieTimer().startTimer
    val metadataPartitions = metadataReader.getAllPartitionPaths
    logDebug("Listing partitions Took " + timer.endTimer + " ms")
    val fsPartitions = fsMetaReader.getAllPartitionPaths
    Collections.sort(fsPartitions)
    Collections.sort(metadataPartitions)

    val allPartitions = new util.HashSet[String]
    allPartitions.addAll(fsPartitions)
    allPartitions.addAll(metadataPartitions)

    if (!fsPartitions.equals(metadataPartitions)) {
      logError("FS partition listing is not matching with metadata partition listing!")
      logError("All FS partitions: " + util.Arrays.toString(fsPartitions.toArray))
      logError("All Metadata partitions: " + util.Arrays.toString(metadataPartitions.toArray))
    }

    val rows = new util.ArrayList[Row]
    for (partition <- allPartitions) {
      val fileStatusMap = new util.HashMap[String, FileStatus]
      val metadataFileStatusMap = new util.HashMap[String, FileStatus]
      val metadataStatuses = metadataReader.getAllFilesInPartition(new Path(basePath, partition))
      util.Arrays.stream(metadataStatuses).iterator().asScala.foreach((entry: FileStatus) => metadataFileStatusMap.put(entry.getPath.getName, entry))
      val fsStatuses = fsMetaReader.getAllFilesInPartition(new Path(basePath, partition))
      util.Arrays.stream(fsStatuses).iterator().asScala.foreach((entry: FileStatus) => fileStatusMap.put(entry.getPath.getName, entry))
      val allFiles = new util.HashSet[String]
      allFiles.addAll(fileStatusMap.keySet)
      allFiles.addAll(metadataFileStatusMap.keySet)
      for (file <- allFiles) {
        val fsFileStatus = fileStatusMap.get(file)
        val metaFileStatus = metadataFileStatusMap.get(file)
        val doesFsFileExists = fsFileStatus != null
        val doesMetadataFileExists = metaFileStatus != null
        val fsFileLength = if (doesFsFileExists) fsFileStatus.getLen else 0
        val metadataFileLength = if (doesMetadataFileExists) metaFileStatus.getLen else 0
        if (verbose) { // if verbose print all files
          rows.add(Row(partition, file, doesFsFileExists, doesMetadataFileExists, fsFileLength, metadataFileLength))
        } else if ((doesFsFileExists != doesMetadataFileExists) || (fsFileLength != metadataFileLength)) { // if non verbose, print only non matching files
          rows.add(Row(partition, file, doesFsFileExists, doesMetadataFileExists, fsFileLength, metadataFileLength))
        }
      }
      if (metadataStatuses.length != fsStatuses.length) {
        logError(" FS and metadata files count not matching for " + partition + ". FS files count " + fsStatuses.length + ", metadata base files count " + metadataStatuses.length)
      }
      for (entry <- fileStatusMap.entrySet) {
        if (!metadataFileStatusMap.containsKey(entry.getKey)) {
          logError("FS file not found in metadata " + entry.getKey)
        } else if (entry.getValue.getLen != metadataFileStatusMap.get(entry.getKey).getLen) {
          logError(" FS file size mismatch " + entry.getKey + ", size equality " + (entry.getValue.getLen == metadataFileStatusMap.get(entry.getKey).getLen) + ". FS size " + entry.getValue.getLen + ", metadata size " + metadataFileStatusMap.get(entry.getKey).getLen)
        }
      }
      for (entry <- metadataFileStatusMap.entrySet) {
        if (!fileStatusMap.containsKey(entry.getKey)) {
          logError("Metadata file not found in FS " + entry.getKey)
        } else if (entry.getValue.getLen != fileStatusMap.get(entry.getKey).getLen) {
          logError(" Metadata file size mismatch " + entry.getKey + ", size equality " + (entry.getValue.getLen == fileStatusMap.get(entry.getKey).getLen) + ". Metadata size " + entry.getValue.getLen + ", FS size " + metadataFileStatusMap.get(entry.getKey).getLen)
        }
      }
    }
    rows.stream().toArray().map(r => r.asInstanceOf[Row]).toList
  }

  override def build: Procedure = new ValidateMetadataTableFilesProcedure()
}

object ValidateMetadataTableFilesProcedure {
  val NAME = "validate_metadata_table_files"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ValidateMetadataTableFilesProcedure()
  }
}
