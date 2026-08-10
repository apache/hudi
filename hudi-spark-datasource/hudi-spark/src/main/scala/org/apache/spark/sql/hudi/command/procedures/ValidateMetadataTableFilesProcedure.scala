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

import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.util.HoodieTimer
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.metadata.HoodieBackedTableMetadata
import org.apache.hudi.storage.{StoragePath, StoragePathInfo}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.Collections
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ValidateMetadataTableFilesProcedure() extends BaseProcedure with ProcedureBuilder with Logging {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "verbose", DataTypes.BooleanType, false)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_name", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("is_present_in_fs", DataTypes.BooleanType, nullable = true, Metadata.empty),
    StructField("is_present_in_metadata", DataTypes.BooleanType, nullable = true, Metadata.empty),
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
    val metaClient = createMetaClient(jsc, basePath)
    val config = HoodieMetadataConfig.newBuilder.enable(true).build
    val metadataReader = new HoodieBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getStorageConf),
      metaClient.getStorage, config, basePath)

    if (!metadataReader.enabled) {
      throw new HoodieException(s"Metadata Table not enabled/initialized.")
    }

    val fsConfig = HoodieMetadataConfig.newBuilder.enable(false).build
    val fsMetaReader = new HoodieBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getStorageConf),
      metaClient.getStorage, fsConfig, basePath)

    val timer = HoodieTimer.start
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
    for (partition <- allPartitions.asScala) {
      val pathInfoMap = new util.HashMap[String, StoragePathInfo]
      val metadataPathInfoMap = new util.HashMap[String, StoragePathInfo]
      val metadataPathInfoList = metadataReader.getAllFilesInPartition(new StoragePath(basePath, partition))
      metadataPathInfoList.asScala.foreach((entry: StoragePathInfo) => metadataPathInfoMap.put(entry.getPath.getName, entry))
      val pathInfoList = fsMetaReader.getAllFilesInPartition(new StoragePath(basePath, partition))
      pathInfoList.asScala.foreach((entry: StoragePathInfo) => pathInfoMap.put(entry.getPath.getName, entry))
      val allFiles = new util.HashSet[String]
      allFiles.addAll(pathInfoMap.keySet)
      allFiles.addAll(metadataPathInfoMap.keySet)
      for (file <- allFiles.asScala) {
        val fsFileStatus = pathInfoMap.get(file)
        val metaFileStatus = metadataPathInfoMap.get(file)
        val doesFsFileExists = fsFileStatus != null
        val doesMetadataFileExists = metaFileStatus != null
        val fsFileLength = if (doesFsFileExists) fsFileStatus.getLength else 0
        val metadataFileLength = if (doesMetadataFileExists) metaFileStatus.getLength else 0
        if (verbose) { // if verbose print all files
          rows.add(Row(partition, file, doesFsFileExists, doesMetadataFileExists, fsFileLength, metadataFileLength))
        } else if ((doesFsFileExists != doesMetadataFileExists) || (fsFileLength != metadataFileLength)) { // if non verbose, print only non matching files
          rows.add(Row(partition, file, doesFsFileExists, doesMetadataFileExists, fsFileLength, metadataFileLength))
        }
      }
      if (metadataPathInfoList.size() != pathInfoList.size()) {
        logError(" FS and metadata files count not matching for " + partition + ". FS files count " + pathInfoList.size() + ", metadata base files count " + metadataPathInfoList.size())
      }
      for (entry <- pathInfoMap.entrySet.asScala) {
        if (!metadataPathInfoMap.containsKey(entry.getKey)) {
          logError("FS file not found in metadata " + entry.getKey)
        } else if (entry.getValue.getLength != metadataPathInfoMap.get(entry.getKey).getLength) {
          logError(" FS file size mismatch " + entry.getKey + ", size equality "
            + (entry.getValue.getLength == metadataPathInfoMap.get(entry.getKey).getLength) + ". FS size "
            + entry.getValue.getLength + ", metadata size " + metadataPathInfoMap.get(entry.getKey).getLength)
        }
      }
      for (entry <- metadataPathInfoMap.entrySet.asScala) {
        if (!pathInfoMap.containsKey(entry.getKey)) {
          logError("Metadata file not found in FS " + entry.getKey)
        } else if (entry.getValue.getLength != pathInfoMap.get(entry.getKey).getLength) {
          logError(" Metadata file size mismatch " + entry.getKey + ", size equality "
            + (entry.getValue.getLength == pathInfoMap.get(entry.getKey).getLength) + ". Metadata size "
            + entry.getValue.getLength + ", FS size " + metadataPathInfoMap.get(entry.getKey).getLength)
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
