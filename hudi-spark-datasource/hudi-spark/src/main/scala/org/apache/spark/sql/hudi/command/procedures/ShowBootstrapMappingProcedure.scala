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

import org.apache.hudi.common.bootstrap.index.BootstrapIndex
import org.apache.hudi.common.model.{BootstrapFileMapping, HoodieFileGroupId}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.exception.HoodieException

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.util
import java.util.function.Supplier

import scala.collection.JavaConverters._

class ShowBootstrapMappingProcedure extends BaseProcedure with ProcedureBuilder {
  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.optional(1, "partition_path", DataTypes.StringType, ""),
    ProcedureParameter.optional(2, "file_ids", DataTypes.StringType, ""),
    ProcedureParameter.optional(3, "limit", DataTypes.IntegerType, 10),
    ProcedureParameter.optional(4, "sort_by", DataTypes.StringType, "partition"),
    ProcedureParameter.optional(5, "desc", DataTypes.BooleanType, false)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("file_id", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("source_base_path", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("source_partition", DataTypes.StringType, nullable = true, Metadata.empty),
    StructField("source_file", DataTypes.StringType, nullable = true, Metadata.empty))
  )

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val tableName = getArgValueOrDefault(args, PARAMETERS(0))
    val partitionPath = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val fileIds = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[Int]
    val sortBy = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[String]
    val desc = getArgValueOrDefault(args, PARAMETERS(5)).get.asInstanceOf[Boolean]

    val basePath: String = getBasePath(tableName)
    val metaClient = createMetaClient(jsc, basePath)

    if (partitionPath.isEmpty && fileIds.nonEmpty) throw new IllegalStateException("PartitionPath is mandatory when passing fileIds.")

    val indexReader = createBootstrapIndexReader(metaClient)
    val indexedPartitions = indexReader.getIndexedPartitionPaths

    if (partitionPath.nonEmpty && !indexedPartitions.contains(partitionPath)) new HoodieException(partitionPath + " is not an valid indexed partition")

    val mappingList: util.ArrayList[BootstrapFileMapping] = new util.ArrayList[BootstrapFileMapping]
    if (fileIds.nonEmpty) {
      val fileGroupIds = fileIds.split(",").toList.map((fileId: String) => new HoodieFileGroupId(partitionPath, fileId)).asJava
      mappingList.addAll(indexReader.getSourceFileMappingForFileIds(fileGroupIds).values)
    } else if (partitionPath.nonEmpty) {
      mappingList.addAll(indexReader.getSourceFileMappingForPartition(partitionPath))
    } else {
      for (part <- indexedPartitions.asScala) {
        mappingList.addAll(indexReader.getSourceFileMappingForPartition(part))
      }
    }

    val rows: java.util.List[Row] = mappingList.asScala
      .map(mapping => Row(mapping.getPartitionPath, mapping.getFileId, mapping.getBootstrapBasePath,
        mapping.getBootstrapPartitionPath, mapping.getBootstrapFileStatus.getPath.getUri)).asJava

    val df = spark.createDataFrame(rows, OUTPUT_TYPE)

    if (desc) {
      df.orderBy(df(sortBy).desc).limit(limit).collect()
    } else {
      df.orderBy(df(sortBy).asc).limit(limit).collect()
    }
  }

  private def createBootstrapIndexReader(metaClient: HoodieTableMetaClient) = {
    val index = BootstrapIndex.getBootstrapIndex(metaClient)
    if (!index.useIndex) throw new HoodieException("This is not a bootstrapped Hudi table. Don't have any index info")
    index.createReader
  }

  override def build: Procedure = new ShowBootstrapMappingProcedure()
}

object ShowBootstrapMappingProcedure {
  val NAME = "show_bootstrap_mapping"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ShowBootstrapMappingProcedure
  }
}